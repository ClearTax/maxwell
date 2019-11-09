package com.zendesk.maxwell.bootstrap;

import com.google.common.collect.Lists;
import com.zendesk.maxwell.CaseSensitivity;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.MaxwellMysqlStatus;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaCapturer;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.schema.columndef.TimeColumnDef;
import com.zendesk.maxwell.scripting.Scripting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SynchronousBootstrapper {
	static final Logger LOGGER = LoggerFactory.getLogger(SynchronousBootstrapper.class);
	private static final long INSERTED_ROWS_UPDATE_PERIOD_MILLIS = 250;
	private final MaxwellContext context;

	private long lastInsertedRowsUpdateTimeMillis = 0;

	public SynchronousBootstrapper(MaxwellContext context) {
		this.context = context;
	}


	public void startBootstrap(BootstrapTask task, AbstractProducer producer, Long currentSchemaID) throws Exception {
		performBootstrap(task, producer, currentSchemaID);
		completeBootstrap(task, producer);
	}

	private Schema captureSchemaForBootstrap(BootstrapTask task) throws SQLException {
		try ( Connection cx = getConnection(task.database) ) {
			CaseSensitivity s = MaxwellMysqlStatus.captureCaseSensitivity(cx);
			SchemaCapturer c = new SchemaCapturer(cx, s, task.database, task.table);
			return c.capture();
		}
	}

	public void performBootstrap(BootstrapTask task, AbstractProducer producer, Long currentSchemaID) throws Exception {
		LOGGER.debug("bootstrapping requested for " + task.logString());

		Schema schema = captureSchemaForBootstrap(task);
		Database database = findDatabase(schema, task.database);
		Table table = findTable(task.table, database);

		producer.push(bootstrapStartRowMap(table));
		LOGGER.info(String.format("bootstrapping started for %s.%s", task.database, task.table));
		int insertedRows;

		try ( Connection connection = getMaxwellConnection();
			  Connection streamingConnection = getStreamingConnection(task.database)) {
			setBootstrapRowToStarted(task.id, connection);

			if (Objects.isNull(table.getPKList()) || table.getPKList().size() != 1) {
				insertedRows = bootstrapWithoutOffsetQuery(streamingConnection, table, task,
						currentSchemaID, producer, connection);
			} else {
				insertedRows = bootstrapWithOffsetQuery(streamingConnection, table, task,
						currentSchemaID, producer, connection);
			}

			setBootstrapRowToCompleted(insertedRows, task.id, connection);
		} catch ( NoSuchElementException e ) {
			LOGGER.info("bootstrapping aborted for " + task.logString());
		}
	}

	private int bootstrapWithOffsetQuery(final Connection streamingConnection, final Table table,
										 final BootstrapTask task, Long currentSchemaID,
										 final AbstractProducer producer,
										 final Connection maxwellConnection) throws Exception {
		int insertedRows = 0;
		lastInsertedRowsUpdateTimeMillis = 0;
		Object lastPKValue = null;
		boolean firstQuery = true;
		boolean allDone = false;
		final int THREAD_POOL_SIZE = task.parallelism;
		final Collection<Callable<Void>> callables = Lists.newArrayListWithExpectedSize(THREAD_POOL_SIZE);
		List<Future<Void>> futures;
		ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

		String bootstrapSql = getBootstrapSql(table, task, null);
		PreparedStatement preparedStatement = streamingConnection.prepareStatement(
				bootstrapSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

		LOGGER.info("Starting bootstrap using offset queries!");

		while (true) {
			final ResultSet resultSet = preparedStatement.executeQuery();

			if (!resultSet.last()) {
				allDone = true;
			} else {
				lastPKValue = getPkValue(resultSet, table);

				if (resultSet.getRow() < task.batchSize)
					allDone = true;

				insertedRows += resultSet.getRow();

				resultSet.beforeFirst(); // Reset the cursor to just before first row
				callables.add(() -> {
					fetchResultSetData(resultSet, table, currentSchemaID, producer);
					return null;
				});
			}

			if (callables.size() == THREAD_POOL_SIZE || allDone) {
				futures = executorService.invokeAll(callables);
				futures.forEach(future -> {
					try {
						future.get();
					} catch (Exception e) {}
				});
				callables.clear();
				updateInsertedRowsColumn(insertedRows, task.id, maxwellConnection);
			}

			if (allDone) break;

			if (firstQuery) {
				firstQuery = false;
				bootstrapSql = getBootstrapSql(table, task, lastPKValue);
			}

			preparedStatement = streamingConnection.prepareStatement(
					bootstrapSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY
			);
			preparedStatement.setObject(1, lastPKValue);
		}
		executorService.shutdown();
		System.gc(); // Releasing unused heap space to OS
		return insertedRows;
	}

	private void fetchResultSetData(final ResultSet resultSet, final Table table,
								   	final Long currentSchemaID, final AbstractProducer producer) {
		try {
			while (resultSet.next()) {
				RowMap row = bootstrapEventRowMap("bootstrap-insert", table.database, table.name, table.getPKList());
				setRowValues(row, resultSet, table);
				row.setSchemaId(currentSchemaID);

				Scripting scripting = context.getConfig().scripting;
				if ( scripting != null )
					scripting.invoke(row);

				if ( LOGGER.isDebugEnabled() )
					LOGGER.debug("bootstrapping row : " + row.toJSON());

				producer.push(row);
			}
			resultSet.close();
		} catch (final Exception e) {
			LOGGER.error("Error while fetching data from resultset ", e);
		}
	}

	private String getBootstrapSql(final Table table, final BootstrapTask task, Object pkValue) {
		final String pkColumnName = table.getPKList().get(0);
		boolean flag = false;

		String sql = String.format("select * from `%s`.%s ", table.database, task.table);

		if (task.whereClause != null && !task.whereClause.equals("")) {
			sql += "where " + task.whereClause + " ";
			flag = true;
		}

		if (Objects.nonNull(pkValue)) {
			if (!flag)
				sql += "where ";
			else
				sql += "and ";

			sql += pkColumnName + " > ? ";
		}

		sql += String.format("order by %s limit %s", pkColumnName, task.batchSize.toString());

		return sql;
	}

	private int bootstrapWithoutOffsetQuery(final Connection streamingConnection, final Table table,
											 final BootstrapTask task, Long currentSchemaID,
											 final AbstractProducer producer,
											 final Connection maxwellConnection) throws Exception {
		LOGGER.info("Starting bootstrap without using offset queries!");
		ResultSet resultSet = getAllRows(task.database, task.table, table, task.whereClause, streamingConnection);
		int insertedRows = 0;
		lastInsertedRowsUpdateTimeMillis = 0; // ensure updateInsertedRowsColumn is called at least once
		while ( resultSet.next() ) {
			RowMap row = bootstrapEventRowMap("bootstrap-insert", table.database, table.name, table.getPKList());
			setRowValues(row, resultSet, table);
			row.setSchemaId(currentSchemaID);

			Scripting scripting = context.getConfig().scripting;
			if ( scripting != null )
				scripting.invoke(row);

			if ( LOGGER.isDebugEnabled() )
				LOGGER.debug("bootstrapping row : " + row.toJSON());

			producer.push(row);
			++insertedRows;
			updateInsertedRowsColumn(insertedRows, task.id, maxwellConnection);
		}
		return insertedRows;
	}

	private void updateInsertedRowsColumn(int insertedRows, Long id, Connection connection) throws SQLException, NoSuchElementException {
		long now = System.currentTimeMillis();
		if ( now - lastInsertedRowsUpdateTimeMillis > INSERTED_ROWS_UPDATE_PERIOD_MILLIS ) {
			String sql = "update `bootstrap` set inserted_rows = ? where id = ?";
			PreparedStatement preparedStatement = connection.prepareStatement(sql);
			preparedStatement.setInt(1, insertedRows);
			preparedStatement.setLong(2, id);
			if ( preparedStatement.executeUpdate() == 0 ) {
				throw new NoSuchElementException();
			}
			lastInsertedRowsUpdateTimeMillis = now;
		}
	}

	protected Connection getConnection(String databaseName) throws SQLException {
		Connection conn = context.getReplicationConnection();
		conn.setCatalog(databaseName);
		return conn;
	}

	protected Connection getStreamingConnection(String databaseName) throws SQLException, URISyntaxException {
		Connection conn = DriverManager.getConnection(context.getConfig().replicationMysql.getConnectionURI(false), context.getConfig().replicationMysql.user, context.getConfig().replicationMysql.password);
		conn.setCatalog(databaseName);
		return conn;
	}

	protected Connection getMaxwellConnection() throws SQLException {
		Connection conn = context.getMaxwellConnection();
		conn.setCatalog(context.getConfig().databaseName);
		return conn;
	}

	private RowMap bootstrapStartRowMap(Table table) {
		return bootstrapEventRowMap("bootstrap-start", table.database, table.name, table.getPKList());
	}

	private RowMap bootstrapEventRowMap(String type, String db, String tbl, List<String> pkList) {
		return new RowMap(
			type,
			db,
			tbl,
			System.currentTimeMillis(),
			pkList,
			null);
	}

	public void completeBootstrap(BootstrapTask task, AbstractProducer producer) throws Exception {
		producer.push(bootstrapEventRowMap("bootstrap-complete", task.database, task.table, new ArrayList<>()));
		LOGGER.info("bootstrapping ended for " + task.logString());
	}

	private Table findTable(String tableName, Database database) {
		Table table = database.findTable(tableName);
		if ( table == null )
			throw new RuntimeException("Couldn't find table " + tableName);
		return table;
	}

	private Database findDatabase(Schema schema, String databaseName) {
		Database database = schema.findDatabase(databaseName);
		if ( database == null )
			throw new RuntimeException("Couldn't find database " + databaseName);
		return database;
	}

	private ResultSet getAllRows(String databaseName, String tableName, Table table, String whereClause,
								 Connection connection) throws SQLException {
		Statement statement = createBatchStatement(connection);
		String pk = table.getPKString();

		String sql = String.format("select * from `%s`.%s", databaseName, tableName);

		if ( whereClause != null && !whereClause.equals("") ) {
			sql += String.format(" where %s", whereClause);
		}

		if ( pk != null && !pk.equals("") ) {
			sql += String.format(" order by %s", pk);
		}

		return statement.executeQuery(sql);
	}

	private Statement createBatchStatement(Connection connection) throws SQLException {
		Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		statement.setFetchSize(Integer.MIN_VALUE);
		return statement;
	}

	private final String startBootstrapSQL = "update `bootstrap` set started_at=NOW() where id=?";
	private void setBootstrapRowToStarted(Long id, Connection connection) throws SQLException, NoSuchElementException {
		PreparedStatement preparedStatement = connection.prepareStatement(startBootstrapSQL);
		preparedStatement.setLong(1, id);
		if ( preparedStatement.executeUpdate() == 0) {
			throw new NoSuchElementException();
		}
	}

	private final String completeBootstrapSQL = "update `bootstrap` set is_complete=1, inserted_rows=?, completed_at=NOW() where id=?";
	private void setBootstrapRowToCompleted(int insertedRows, Long id, Connection connection) throws SQLException, NoSuchElementException {
		PreparedStatement preparedStatement = connection.prepareStatement(completeBootstrapSQL);
		preparedStatement.setInt(1, insertedRows);
		preparedStatement.setLong(2, id);
		if ( preparedStatement.executeUpdate() == 0) {
			throw new NoSuchElementException();
		}
	}

	private Object getPkValue(final ResultSet resultSet, final Table table) throws SQLException {
		String pkColumnName = "";
		Object pkValue = null;

		if (Objects.nonNull(table.getPKList()) && table.getPKList().size() == 1)
			pkColumnName = table.getPKList().get(0);

		for (int index = 0; index < table.getColumnList().size(); ++index) {
			ColumnDef columnDef = table.getColumnList().get(index);
			if (columnDef.getName().equals(pkColumnName)) {
				if ( columnDef instanceof TimeColumnDef )
					pkValue = resultSet.getTimestamp(index + 1);
				else
					pkValue = resultSet.getObject(index + 1);
			}
		}
		return pkValue;
	}

	private void setRowValues(RowMap row, ResultSet resultSet, Table table) throws SQLException {
		Iterator<ColumnDef> columnDefinitions = table.getColumnList().iterator();
		int columnIndex = 1;

		while ( columnDefinitions.hasNext() ) {
			ColumnDef columnDefinition = columnDefinitions.next();
			Object columnValue;

			// need to explicitly coerce TIME into TIMESTAMP in order to preserve nanoseconds
			if ( columnDefinition instanceof TimeColumnDef )
				columnValue = resultSet.getTimestamp(columnIndex);
			else
				columnValue = resultSet.getObject(columnIndex);

			row.putData(
				columnDefinition.getName(),
				columnValue == null ? null : columnDefinition.asJSON(columnValue, new MaxwellOutputConfig())
			);

			++columnIndex;
		}
	}
}

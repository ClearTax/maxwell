package com.zendesk.maxwell.producer;

import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.partitioners.MaxwellKinesisPartitioner;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;

import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;

import com.zendesk.maxwell.util.S3Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KinesisCallback implements FutureCallback<UserRecordResult> {
	public static final Logger logger = LoggerFactory.getLogger(KinesisCallback.class);

	private final AbstractAsyncProducer.CallbackCompleter cc;
	private final Position position;
	private final String json;
	private MaxwellContext context;
	private final String key;
	private Counter succeededMessageCount;
	private Counter failedMessageCount;
	private Meter succeededMessageMeter;
	private Meter failedMessageMeter;

	public KinesisCallback(AbstractAsyncProducer.CallbackCompleter cc, Position position, String key, String json,
						   Counter producedMessageCount, Counter failedMessageCount, Meter producedMessageMeter,
						   Meter failedMessageMeter, MaxwellContext context) {
		this.cc = cc;
		this.position = position;
		this.key = key;
		this.json = json;
		this.succeededMessageCount = producedMessageCount;
		this.failedMessageCount = failedMessageCount;
		this.succeededMessageMeter = producedMessageMeter;
		this.failedMessageMeter = failedMessageMeter;
		this.context = context;
	}

	@Override
	public void onFailure(Throwable t) {
		logger.error(t.getClass().getSimpleName() + " @ " + position + " -- " + key);
		logger.error(t.getLocalizedMessage());
		this.failedMessageCount.inc();
		this.failedMessageMeter.mark();

		if(t instanceof UserRecordFailedException) {
			Attempt last = Iterables.getLast(((UserRecordFailedException) t).getResult().getAttempts());
			logger.error(String.format("Record failed to put - %s : %s", last.getErrorCode(), last.getErrorMessage()));
		}

		logger.error("Exception during put", t);

		if (!context.getConfig().ignoreProducerError) {
			context.terminate(new RuntimeException(t));
		} else {
			cc.markCompleted();
		}
	};

	@Override
	public void onSuccess(UserRecordResult result) {
		this.succeededMessageCount.inc();
		this.succeededMessageMeter.mark();
		if(logger.isDebugEnabled()) {
			logger.debug("->  key:" + key + ", shard id:" + result.getShardId() + ", sequence number:" + result.getSequenceNumber());
			logger.debug("   " + json);
			logger.debug("   " + position);
			logger.debug("");
		}

		cc.markCompleted();
	};
}

public class MaxwellKinesisProducer extends AbstractAsyncProducer {
	private static final Logger logger = LoggerFactory.getLogger(MaxwellKinesisProducer.class);

	private final MaxwellKinesisPartitioner partitioner;
	private final KinesisProducer kinesisProducer;
	private final String kinesisStream;
	private final boolean s3Enable;
	private final String s3Bucket;
	private S3Util s3Util;

	private final String PATH_DELIMITTER = "/";

	public MaxwellKinesisProducer(MaxwellContext context, String kinesisStream,
								  boolean s3Enable, String s3Bucket, Regions region) {
		super(context);

		String partitionKey = context.getConfig().producerPartitionKey;
		String partitionColumns = context.getConfig().producerPartitionColumns;
		String partitionFallback = context.getConfig().producerPartitionFallback;
		boolean kinesisMd5Keys = context.getConfig().kinesisMd5Keys;
		this.partitioner = new MaxwellKinesisPartitioner(partitionKey, partitionColumns, partitionFallback, kinesisMd5Keys);
		this.kinesisStream = kinesisStream;
		this.s3Enable = s3Enable;
		this.s3Bucket = s3Bucket;

		if (s3Enable) {
			final AmazonS3 amazonS3 = AmazonS3ClientBuilder.standard()
					.withCredentials(new InstanceProfileCredentialsProvider(false))
					.withRegion(region)
					.build();

			this.s3Util = new S3Util(amazonS3);
		}

		Path path = Paths.get("kinesis-producer-library.properties");
		if(Files.exists(path) && Files.isRegularFile(path)) {
			KinesisProducerConfiguration config = KinesisProducerConfiguration.fromPropertiesFile(path.toString());
			this.kinesisProducer = new KinesisProducer(config);
		} else {
			this.kinesisProducer = new KinesisProducer();
		}
	}

	@Override
	public void sendAsync(RowMap r, AbstractAsyncProducer.CallbackCompleter cc) throws Exception {
		String key = this.partitioner.getKinesisKey(r);
		String value = r.toJSON(outputConfig);

		if (s3Enable && isLargerThanKinesisLimit(value)) {
			value = uploadToS3(value);
		}

		ByteBuffer encodedValue = ByteBuffer.wrap(value.getBytes("UTF-8"));

		// release the reference to ease memory pressure
		if(!KinesisCallback.logger.isDebugEnabled()) {
			value = null;
		}

		FutureCallback<UserRecordResult> callback = new KinesisCallback(cc, r.getNextPosition(), key, value,
				this.succeededMessageCount, this.failedMessageCount, this.succeededMessageMeter, this.failedMessageMeter, this.context);

		try {
			ListenableFuture<UserRecordResult> future = kinesisProducer.addUserRecord(kinesisStream, key, encodedValue);
			Futures.addCallback(future, callback);
		} catch(IllegalArgumentException t) {
			callback.onFailure(t);
			logger.error("Database:" + r.getDatabase() + ", Table:" + r.getTable() + ", PK:" + r.getRowIdentity().toConcatString() + ", Size:" + Integer.toString(value.length()));
		}
	}

	private boolean isLargerThanKinesisLimit(final String value) {
		return value.length() > 700000; // for 7lk characters it'll exceed the kinesis limit (around 0.93MB)
	}

	private String uploadToS3(final String value) {
		final String keyName = Joiner.on(PATH_DELIMITTER).join(
				"binlog_payloads",
				UUID.randomUUID().toString() + ".txt"
		);
		final URL url = s3Util.upload(
				s3Bucket,
				keyName,
				value
		);

		return url.toString();
	}

	public void close() {
		this.kinesisProducer.destroy();
	}
}

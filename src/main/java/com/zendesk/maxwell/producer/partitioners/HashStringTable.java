package com.zendesk.maxwell.producer.partitioners;

import com.zendesk.maxwell.row.RowMap;

/**
 * Created by kaufmannkr on 1/18/16.
 */
public class HashStringTable implements HashStringProvider {
	public String getHashString(RowMap r) {
		return r.getTable();
	}
}

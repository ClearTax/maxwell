package com.zendesk.maxwell.producer.partitioners;

import com.zendesk.maxwell.row.RowMap;

/**
 * Created by kaufmannkr on 1/21/16.
 */
public interface HashStringProvider {
	String getHashString(RowMap r);
}

package com.datorama.oss.timbermill.common;

import java.util.List;

public interface DiskHandler {
	List<DbBulkRequest> fetchAndDeleteFailedBulks();

	void persistToDisk(DbBulkRequest dbBulkRequest);

	boolean hasFailedBulks();

	boolean isCreatedSuccefully();
}


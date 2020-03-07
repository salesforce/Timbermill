package com.datorama.oss.timbermill.common;

import java.util.List;

public interface DiskHandler {
	List<DbBulkRequest> fetchFailedBulks();

	void persistToDisk(DbBulkRequest dbBulkRequest);

	void deleteBulk(DbBulkRequest dbBulkRequest);

	void updateBulk(String id, DbBulkRequest dbBulkRequest);
}


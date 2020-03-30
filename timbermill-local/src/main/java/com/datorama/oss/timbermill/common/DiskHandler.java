package com.datorama.oss.timbermill.common;

import java.util.List;

import com.datorama.oss.timbermill.common.exceptions.MaximunInsertTriesException;

public interface DiskHandler {
	List<DbBulkRequest> fetchAndDeleteFailedBulks();

	void persistToDisk(DbBulkRequest dbBulkRequest) throws MaximunInsertTriesException;

	boolean hasFailedBulks();

	boolean isCreatedSuccefully();
}


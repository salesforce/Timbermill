package com.datorama.oss.timbermill.common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datorama.oss.timbermill.common.exceptions.MaximumInsertTriesException;

public interface DiskHandler {

	List<DbBulkRequest> fetchAndDeleteFailedBulks();

	void persistToDisk(DbBulkRequest dbBulkRequest) throws MaximumInsertTriesException;

	boolean hasFailedBulks();

	boolean isCreatedSuccessfully();

	static Map<String, Object> buildDiskHandlerParams(int maxFetchedBulksInOneTime, int maxInsertTries, String locationInDisk) {
		Map<String, Object> diskHandlerParams = new HashMap<>();
		diskHandlerParams.put(SQLJetDiskHandler.MAX_FETCHED_BULKS_IN_ONE_TIME, maxFetchedBulksInOneTime);
		diskHandlerParams.put(SQLJetDiskHandler.MAX_INSERT_TRIES, maxInsertTries);
		diskHandlerParams.put(SQLJetDiskHandler.LOCATION_IN_DISK, locationInDisk);
		return diskHandlerParams;
	}

	int failedBulksAmount();

	void close();
}


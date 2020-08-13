package com.datorama.oss.timbermill.common.disk;

import java.util.*;

import com.datorama.oss.timbermill.common.exceptions.MaximumInsertTriesException;
import com.datorama.oss.timbermill.unit.Event;

public interface DiskHandler {

	List<DbBulkRequest> fetchAndDeleteFailedBulks(String flowId);

	List<Event> fetchAndDeleteOverflowedEvents(String flowId);

	void persistBulkRequestToDisk(DbBulkRequest dbBulkRequest, String flowId, int bulkNum) throws MaximumInsertTriesException;

	void persistEventsToDisk(ArrayList<Event> events);

	boolean hasFailedBulks(String flowId);

	boolean isCreatedSuccessfully();

	static Map<String, Object> buildDiskHandlerParams(int maxFetchedBulksInOneTime, int maxInsertTries, String locationInDisk) {
		Map<String, Object> diskHandlerParams = new HashMap<>();
		diskHandlerParams.put(SQLJetDiskHandler.MAX_FETCHED_BULKS_IN_ONE_TIME, maxFetchedBulksInOneTime);
		diskHandlerParams.put(SQLJetDiskHandler.MAX_INSERT_TRIES, maxInsertTries);
		diskHandlerParams.put(SQLJetDiskHandler.LOCATION_IN_DISK, locationInDisk);
		return diskHandlerParams;
	}

	long failedBulksAmount();

	long overFlowedEventsAmount();

	void close();
}


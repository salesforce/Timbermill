package com.datorama.oss.timbermill.common.disk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datorama.oss.timbermill.common.exceptions.MaximumInsertTriesException;
import com.datorama.oss.timbermill.unit.Event;

public interface DiskHandler {

	List<DbBulkRequest> fetchAndDeleteFailedBulks();

	List<Event> fetchAndDeleteOverflowedEvents();

	void persistBulkRequestToDisk(DbBulkRequest dbBulkRequest) throws MaximumInsertTriesException;

	void persistEventsToDisk(ArrayList<Event> events);

	boolean hasFailedBulks();

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


package com.datorama.oss.timbermill.common.disk;

import java.util.*;
import java.util.concurrent.BlockingQueue;

import com.datorama.oss.timbermill.common.KamonConstants;
import com.datorama.oss.timbermill.unit.Event;
import com.google.common.collect.Lists;

public abstract class selfHealingHandler {

	public abstract List<DbBulkRequest> fetchAndDeleteFailedBulks();

	public abstract List<Event> fetchAndDeleteOverflowedEvents();

	public abstract void persistBulkRequestToDisk(DbBulkRequest dbBulkRequest, int bulkNum);

	abstract void persistEventsToDisk(ArrayList<Event> events);

	public abstract boolean hasFailedBulks();

	public abstract boolean isCreatedSuccessfully();

	public static Map<String, Object> buildDiskHandlerParams(int maxFetchedBulksInOneTime, int maxInsertTries, String locationInDisk) {
		Map<String, Object> diskHandlerParams = new HashMap<>();
		diskHandlerParams.put(SQLJetHandler.MAX_FETCHED_BULKS_IN_ONE_TIME, maxFetchedBulksInOneTime);
		diskHandlerParams.put(SQLJetHandler.MAX_INSERT_TRIES, maxInsertTries);
		diskHandlerParams.put(SQLJetHandler.LOCATION_IN_DISK, locationInDisk);
		return diskHandlerParams;
	}

	abstract long failedBulksAmount();

	abstract long overFlowedEventsAmount();

	public abstract void close();

	public void spillOverflownEventsToDisk(BlockingQueue<Event> overflowedQueue) {
		while (!overflowedQueue.isEmpty()) {
			ArrayList<Event> events = Lists.newArrayList();
			overflowedQueue.drainTo(events, 100000);
			KamonConstants.MESSAGES_IN_OVERFLOWED_QUEUE_RANGE_SAMPLER.withoutTags().decrement(events.size());
			persistEventsToDisk(events);
		}
	}
}


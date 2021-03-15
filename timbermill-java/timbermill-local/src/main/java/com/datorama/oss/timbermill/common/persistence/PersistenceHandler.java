package com.datorama.oss.timbermill.common.persistence;

import java.util.*;
import java.util.concurrent.BlockingQueue;

import com.datorama.oss.timbermill.common.KamonConstants;
import com.datorama.oss.timbermill.unit.Event;
import com.google.common.collect.Lists;

public abstract class PersistenceHandler {

	public abstract List<DbBulkRequest> fetchAndDeleteFailedBulks();

	public abstract List<Event> fetchAndDeleteOverflowedEvents();

	public abstract void persistBulkRequest(DbBulkRequest dbBulkRequest, int bulkNum);

	abstract void persistEvents(ArrayList<Event> events);

	public abstract boolean hasFailedBulks();

	public abstract boolean isCreatedSuccessfully();

	public static Map<String, Object> buildPersistenceHandlerParams(int maxFetchedBulksInOneTime, int maxInsertTries, String location) {
		Map<String, Object> persistenceHandlerParams = new HashMap<>();
		persistenceHandlerParams.put(SQLJetPersistenceHandler.MAX_FETCHED_BULKS_IN_ONE_TIME, maxFetchedBulksInOneTime);
		persistenceHandlerParams.put(SQLJetPersistenceHandler.MAX_INSERT_TRIES, maxInsertTries);
		persistenceHandlerParams.put(SQLJetPersistenceHandler.LOCATION_IN_DISK, location);
		return persistenceHandlerParams;
	}

	abstract long failedBulksAmount();

	abstract long overFlowedEventsAmount();

	public abstract void close();

	public void spillOverflownEvents(BlockingQueue<Event> overflowedQueue) {
		while (!overflowedQueue.isEmpty()) {
			ArrayList<Event> events = Lists.newArrayList();
			overflowedQueue.drainTo(events, 100000);
			KamonConstants.MESSAGES_IN_OVERFLOWED_QUEUE_RANGE_SAMPLER.withoutTags().decrement(events.size());
			persistEvents(events);
		}
	}
}


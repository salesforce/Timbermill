package com.datorama.oss.timbermill.common.persistence;

import com.datorama.oss.timbermill.RedisCacheConfig;
import com.datorama.oss.timbermill.common.KamonConstants;
import com.datorama.oss.timbermill.unit.Event;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public abstract class PersistenceHandler {

	static final String MAX_FETCHED_BULKS_IN_ONE_TIME = "MAX_FETCHED_BULKS_IN_ONE_TIME";
	static final String MAX_INSERT_TRIES = "MAX_INSERT_TRIES";

	protected int maxFetchedBulksInOneTime;
	protected int maxInsertTries;

	PersistenceHandler(int maxFetchedBulksInOneTime, int maxInsertTries){
		this.maxFetchedBulksInOneTime = maxFetchedBulksInOneTime;
		this.maxInsertTries = maxInsertTries;
	}

	public abstract List<DbBulkRequest> fetchAndDeleteFailedBulks();

	public abstract List<Event> fetchAndDeleteOverflowedEvents();

	public abstract void persistBulkRequest(DbBulkRequest dbBulkRequest, int bulkNum);

	abstract void persistEvents(ArrayList<Event> events);

	public abstract boolean hasFailedBulks();

	public abstract boolean isCreatedSuccessfully();

	public static Map<String, Object> buildPersistenceHandlerParams(int maxFetchedBulksInOneTime, int maxInsertTries, String locationInDisk, RedisCacheConfig redisCacheConfig) {
		Map<String, Object> persistenceHandlerParams = new HashMap<>();
		persistenceHandlerParams.put(PersistenceHandler.MAX_FETCHED_BULKS_IN_ONE_TIME, maxFetchedBulksInOneTime);
		persistenceHandlerParams.put(PersistenceHandler.MAX_INSERT_TRIES, maxInsertTries);
		persistenceHandlerParams.put(SQLJetPersistenceHandler.LOCATION_IN_DISK, locationInDisk);
		persistenceHandlerParams.put(RedisPersistenceHandler.REDIS_CONFIG, redisCacheConfig);
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


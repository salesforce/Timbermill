package com.datorama.oss.timbermill.common.persistence;

import com.datorama.oss.timbermill.common.redis.RedisServiceConfig;
import com.datorama.oss.timbermill.common.KamonConstants;
import com.datorama.oss.timbermill.unit.Event;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;

public abstract class PersistenceHandler {

	static final String MAX_FETCHED_BULKS_IN_ONE_TIME = "MAX_FETCHED_BULKS_IN_ONE_TIME";
	static final String MAX_FETCHED_EVENTS_IN_ONE_TIME = "MAX_FETCHED_EVENTS_IN_ONE_TIME";
	static final String MAX_INSERT_TRIES = "MAX_INSERT_TRIES";

	protected int maxFetchedBulksInOneTime;
	protected int maxFetchedEventsListsInOneTime;
	protected int maxInsertTries;

	PersistenceHandler(int maxFetchedBulksInOneTime, int maxFetchedEventsListsInOneTime, int maxInsertTries){
		this.maxFetchedBulksInOneTime = maxFetchedBulksInOneTime;
		this.maxFetchedEventsListsInOneTime = maxFetchedEventsListsInOneTime;
		this.maxInsertTries = maxInsertTries;
	}

	public abstract List<DbBulkRequest> fetchAndDeleteFailedBulks();

	public abstract List<Event> fetchAndDeleteOverflowedEvents();

	public abstract Future<?> persistBulkRequest(DbBulkRequest dbBulkRequest, int bulkNum);

	public abstract void persistEvents(ArrayList<Event> events);

	public abstract boolean hasFailedBulks();

	public abstract boolean isCreatedSuccessfully();

	public abstract long failedBulksAmount();

	public abstract long overFlowedEventsListsAmount();

	public abstract void close();

	public abstract void reset();

	public int getMaxFetchedBulksInOneTime() {
		return maxFetchedBulksInOneTime;
	}

	public int getMaxFetchedEventsListsInOneTime() {
		return maxFetchedEventsListsInOneTime;
	}

	public static Map<String, Object> buildPersistenceHandlerParams(int maxFetchedBulksInOneTime, int maxFetchedEventsInOneTime, int maxInsertTries, String locationInDisk, long minLifetime, RedisServiceConfig redisServiceConfig) {
		Map<String, Object> persistenceHandlerParams = new HashMap<>();
		persistenceHandlerParams.put(PersistenceHandler.MAX_FETCHED_BULKS_IN_ONE_TIME, maxFetchedBulksInOneTime);
		persistenceHandlerParams.put(PersistenceHandler.MAX_FETCHED_EVENTS_IN_ONE_TIME, maxFetchedEventsInOneTime);
		persistenceHandlerParams.put(PersistenceHandler.MAX_INSERT_TRIES, maxInsertTries);
		persistenceHandlerParams.put(SQLJetPersistenceHandler.LOCATION_IN_DISK, locationInDisk);
		persistenceHandlerParams.put(RedisPersistenceHandler.MIN_LIFETIME, minLifetime);
		persistenceHandlerParams.put(RedisPersistenceHandler.REDIS_CONFIG, redisServiceConfig);
		return persistenceHandlerParams;
	}

	public void spillOverflownEvents(BlockingQueue<Event> overflowedQueue) {
		while (!overflowedQueue.isEmpty()) {
			ArrayList<Event> events = Lists.newArrayList();
			overflowedQueue.drainTo(events, 100000);
			KamonConstants.MESSAGES_IN_OVERFLOWED_QUEUE_RANGE_SAMPLER.withoutTags().decrement(events.size());
			persistEvents(events);
		}
	}
}


package com.datorama.oss.timbermill.common.persistence;

import com.datorama.oss.timbermill.common.redis.RedisServiceConfig;
import com.datorama.oss.timbermill.common.redis.RedisService;
import com.datorama.oss.timbermill.unit.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class RedisPersistenceHandler extends PersistenceHandler {

    private static ExecutorService executorService = Executors.newFixedThreadPool(1);
    private static final Logger LOG = LoggerFactory.getLogger(RedisPersistenceHandler.class);
    private static final String FAILED_BULKS_QUEUE_NAME = "failed_bulks_queue";
    private static final String OVERFLOWED_EVENTS_QUEUE_NAME = "overflowed_events_queue";
    private static final String FAILED_BULK_PREFIX = "failed_bulk#";
    private static final String OVERFLOW_EVENTS_PREFIX = "overflow_events#";

    public static final String REDIS_CONFIG = "redis_config";

    private RedisService redisService;

    RedisPersistenceHandler(int maxFetchedBulks, int maxFetchedEvents, int maxInsertTries, RedisServiceConfig redisServiceConfig) {
        super(maxFetchedBulks, maxFetchedEvents, maxInsertTries);
        this.redisService = new RedisService(redisServiceConfig);
    }


    @Override
    public List<DbBulkRequest> fetchAndDeleteFailedBulks() {
        LOG.info("Fetching failed bulks from Redis.");

        List<String> ids = new ArrayList<>();
        Map<String, DbBulkRequest> failedBulkRequests = new HashMap<>();
        do {
            ids.clear();

            // pop keys from redis list
            ids.addAll(popElementsFromRedisList(FAILED_BULKS_QUEUE_NAME, maxFetchedBulksInOneTime));
            // get matching failed bulks from redis
            failedBulkRequests.putAll(redisService.getFromRedis(ids));
            // increase times fetched for each fetched one
            failedBulkRequests.values().stream().forEach(dbBulkRequest -> dbBulkRequest.setTimesFetched(dbBulkRequest.getTimesFetched() + 1));
        }
        while (areAllIdsExpired(ids, failedBulkRequests));

        LOG.info("Failed bulks fetch from Redis succeeded. Number of fetched bulks: {}.", failedBulkRequests.size());
        redisService.deleteFromRedis(failedBulkRequests.keySet());
        return new ArrayList<>(failedBulkRequests.values());
    }

    @Override
    public List<Event> fetchAndDeleteOverflowedEvents() {
        LOG.info("Fetching overflowed events from Redis.");

        List<String> ids = new ArrayList<>();
        Map<String, List<Event>> overflowedEventsLists = new HashMap<>();
        do {
            ids.clear();

            // pop keys from redis list
            ids.addAll(popElementsFromRedisList(OVERFLOWED_EVENTS_QUEUE_NAME, maxFetchedEventsListsInOneTime));
            // get matching overflowed events from redis
            overflowedEventsLists.putAll(redisService.getFromRedis(ids));
        }
        while (areAllIdsExpired(ids, overflowedEventsLists));

        redisService.deleteFromRedis(overflowedEventsLists.keySet());
        List<Event> overflowedEvents = combineEventsLists(overflowedEventsLists.values());
        LOG.info("Overflowed events fetch from Redis succeeded. Number of Overflowed events: {}.", overflowedEvents.size());
        return overflowedEvents;
    }

    @Override
    public Future<?> persistBulkRequest(DbBulkRequest dbBulkRequest, int bulkNum) {
        return executorService.submit(() -> {
            LOG.info("Bulk #{} Pushing bulk request to Redis for the {}th time.", bulkNum, dbBulkRequest.getTimesFetched() + 1);
            Map<String, DbBulkRequest> map = new HashMap<>();
            String key = FAILED_BULK_PREFIX + UUID.randomUUID().toString();
            map.put(key, dbBulkRequest);
            if (!(redisService.pushToRedisList(FAILED_BULKS_QUEUE_NAME, key) && redisService.pushToRedisHash(map))) {
                LOG.error("Bulk #{} Failed to persist bulk request to Redis", bulkNum);
            } else {
                LOG.info("Bulk #{} Bulk request was pushed to disk successfully.", bulkNum);
            }
        });
    }

    @Override
    public void persistEvents(ArrayList<Event> events) {
        Map<String, ArrayList<Event>> map = new HashMap<>();
        String key = OVERFLOW_EVENTS_PREFIX + UUID.randomUUID().toString();
        map.put(key, events);
        if (!(redisService.pushToRedisList(OVERFLOWED_EVENTS_QUEUE_NAME, key) && redisService.pushToRedisHash(map))) {
            LOG.error("Failed to persist overflowed events list to Redis");
        } else {
            LOG.info("List of {} overflowed events was pushed successfully to Redis.", events.size());
        }
    }

    @Override
    public boolean hasFailedBulks() {
        int iteration = 0;
        int iterationSize = 100;

        List<String> ids = new ArrayList<>();
        Map<String, DbBulkRequest> failedBulkRequests = new HashMap<>();
        do {
            ids.clear();

            ids.addAll(redisService.getRangeFromRedisList(FAILED_BULKS_QUEUE_NAME, iterationSize * iteration, iterationSize * (iteration + 1) - 1));
            failedBulkRequests.putAll(redisService.getFromRedis(ids));
            iteration += 1;
        }
        while (areAllIdsExpired(ids, failedBulkRequests));

        return failedBulkRequests.size() > 0;
    }

    @Override
    public boolean isCreatedSuccessfully() {
        boolean connected = redisService.isConnected();
        if (!connected){
            LOG.error("Redis wasn't initialized successfully.");
        }
        return connected;
    }

    @Override
    public long failedBulksAmount() {
        int expiredBulkRequestsAmount = 0;
        int iteration = 0;
        int iterationSize = 100;

        List<String> ids = new ArrayList<>();
        Map<String, DbBulkRequest> failedBulkRequests = new HashMap<>();
        do {
            expiredBulkRequestsAmount += ids.size();
            ids.clear();

            ids.addAll(redisService.getRangeFromRedisList(FAILED_BULKS_QUEUE_NAME, iterationSize * iteration, iterationSize * (iteration + 1) - 1));
            failedBulkRequests.putAll(redisService.getFromRedis(ids));
            iteration += 1;
        }
        while (areAllIdsExpired(ids, failedBulkRequests));

        expiredBulkRequestsAmount += (ids.size() - failedBulkRequests.size()); // count expired elements from last iteration
        return redisService.getListLength(FAILED_BULKS_QUEUE_NAME) - expiredBulkRequestsAmount;
    }

    @Override
    public long overFlowedEventsListsAmount() {
        int expiredOverflowedListsAmount = 0;
        int iteration = 0;
        int iterationSize = 100;

        List<String> ids = new ArrayList<>();
        Map<String, List<Event>> eventsLists = new HashMap<>();
        do {
            expiredOverflowedListsAmount += ids.size();
            ids.clear();

            ids.addAll(redisService.getRangeFromRedisList(OVERFLOWED_EVENTS_QUEUE_NAME, iterationSize * iteration, iterationSize * (iteration + 1) - 1));
            eventsLists.putAll(redisService.getFromRedis(ids));
            iteration += 1;
        }
        while (areAllIdsExpired(ids, eventsLists));

        expiredOverflowedListsAmount += (ids.size() - eventsLists.size()); // count expired elements from last iteration
        return redisService.getListLength(OVERFLOWED_EVENTS_QUEUE_NAME) - expiredOverflowedListsAmount;
    }

    @Override
    public void close() {
    }

    @Override
    public void reset() {
        while (fetchAndDeleteFailedBulks().size() > 0) {}
        LOG.info("Finished to reset failed bulk requests in Redis");
        while (fetchAndDeleteOverflowedEvents().size() > 0) {}
        LOG.info("Finished to reset overflowed events in Redis");
    }

    Future<?> persistBulkRequest(DbBulkRequest dbBulkRequest, int bulkNum, int ttl) {
        return executorService.submit(() -> {
            LOG.info("Bulk #{} Pushing bulk request to Redis for the {}th time.", bulkNum, dbBulkRequest.getTimesFetched() + 1);
            Map<String, DbBulkRequest> map = new HashMap<>();
            String key = FAILED_BULK_PREFIX + UUID.randomUUID().toString();
            map.put(key, dbBulkRequest);
            if (!(redisService.pushToRedisList(FAILED_BULKS_QUEUE_NAME, key) && redisService.pushToRedisHash(map, ttl))) {
                LOG.error("Failed to persist bulk request number {} to Redis", bulkNum);
            } else {
                LOG.info("Bulk #{} Bulk request was pushed to disk successfully.", bulkNum);
            }
        });
    }

    void persistEvents(ArrayList<Event> events, int ttl) {
        Map<String, ArrayList<Event>> map = new HashMap<>();
        String key = OVERFLOW_EVENTS_PREFIX + UUID.randomUUID().toString();
        map.put(key, events);
        if (!(redisService.pushToRedisList(OVERFLOWED_EVENTS_QUEUE_NAME, key) && redisService.pushToRedisHash(map, ttl))) {
            LOG.error("Failed to persist overflowed events list to Redis");
        }  else {
            LOG.info("List of {} overflowed events was pushed successfully to Redis.", events.size());
        }
    }

    private List<String> popElementsFromRedisList(String listName, int amount) {
        List<String> elements = redisService.getRangeFromRedisList(listName, 0, amount - 1);
        redisService.trimRedisList(listName, elements.size(), -1);
        return elements;
    }

    private <T> boolean areAllIdsExpired(List<String> ids, Map<String, T> failedBulkRequests) {
        return ids.size() > 0 && failedBulkRequests.size() == 0;
    }

    private List<Event> combineEventsLists(Collection<List<Event>> lists) {
        List<Event> events = new ArrayList<>();
        for (List<Event> list : lists) {
            events.addAll(list);
        }
        return events;
    }
}

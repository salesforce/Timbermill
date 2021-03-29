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

    private static ExecutorService executorService;
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
        executorService = Executors.newFixedThreadPool(1);
    }


    @Override
    public List<DbBulkRequest> fetchAndDeleteFailedBulks() {
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

        redisService.deleteFromRedis(failedBulkRequests.keySet());
        return new ArrayList<>(failedBulkRequests.values());
    }

    @Override
    public List<Event> fetchAndDeleteOverflowedEvents() {
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
        return combineEventsLists(overflowedEventsLists.values());
    }

    @Override
    public Future<?> persistBulkRequest(DbBulkRequest dbBulkRequest, int bulkNum) {
        return executorService.submit(() -> {
            Map<String, DbBulkRequest> map = new HashMap<>();
            String key = FAILED_BULK_PREFIX + UUID.randomUUID().toString();
            map.put(key, dbBulkRequest);
            if (!(redisService.pushToRedisList(FAILED_BULKS_QUEUE_NAME, key) && redisService.pushToRedisHash(map))) {
                LOG.error("Failed to persist bulk request number {} to Redis", bulkNum);
            }
        });
    }

    @Override
    void persistEvents(ArrayList<Event> events) {
        Map<String, ArrayList<Event>> map = new HashMap<>();
        String key = OVERFLOW_EVENTS_PREFIX + UUID.randomUUID().toString();
        map.put(key, events);
        if (!(redisService.pushToRedisList(OVERFLOWED_EVENTS_QUEUE_NAME, key) && redisService.pushToRedisHash(map))) {
            LOG.error("Failed to persist overflowed events list to Redis");
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
        return redisService.isConnected();
    }

    @Override
    long failedBulksAmount() {
        int failedBulkRequestsAmount = 0;
        int iteration = 0;
        int iterationSize = 100;

        List<String> ids = new ArrayList<>();
        Map<String, DbBulkRequest> idsToValues = new HashMap<>();

        do {
            ids.clear();
            idsToValues.clear();

            ids.addAll(redisService.getRangeFromRedisList(FAILED_BULKS_QUEUE_NAME, iterationSize * iteration, iterationSize * (iteration + 1) - 1));
            idsToValues.putAll(redisService.getFromRedis(ids));
            failedBulkRequestsAmount += idsToValues.size();

            iteration += 1;
        }
        while (ids.size() > 0);

        return failedBulkRequestsAmount;
    }

    @Override
    long overFlowedEventsListsAmount() {
        int eventsListsAmount = 0;
        int iteration = 0;
        int iterationSize = 100;

        List<String> ids = new ArrayList<>();
        Map<String, List<Event>> idsToValues = new HashMap<>();

        do {
            ids.clear();
            idsToValues.clear();

            ids.addAll(redisService.getRangeFromRedisList(OVERFLOWED_EVENTS_QUEUE_NAME, iterationSize * iteration, iterationSize * (iteration + 1) - 1));
            idsToValues.putAll(redisService.getFromRedis(ids));
            eventsListsAmount += idsToValues.size();

            iteration += 1;
        }
        while (ids.size() > 0);

        return eventsListsAmount;
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

    public Future<?> persistBulkRequest(DbBulkRequest dbBulkRequest, int bulkNum, int ttl) {
        return executorService.submit(() -> {
            Map<String, DbBulkRequest> map = new HashMap<>();
            String key = FAILED_BULK_PREFIX + UUID.randomUUID().toString();
            map.put(key, dbBulkRequest);
            if (!(redisService.pushToRedisList(FAILED_BULKS_QUEUE_NAME, key) && redisService.pushToRedisHash(map, ttl))) {
                LOG.error("Failed to persist bulk request number {} to Redis", bulkNum);
            }
        });
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

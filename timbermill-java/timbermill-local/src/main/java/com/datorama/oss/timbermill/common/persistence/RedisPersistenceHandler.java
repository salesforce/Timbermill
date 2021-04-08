package com.datorama.oss.timbermill.common.persistence;

import com.datorama.oss.timbermill.common.redis.RedisService;
import com.datorama.oss.timbermill.common.redis.RedisServiceConfig;
import com.datorama.oss.timbermill.unit.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class RedisPersistenceHandler extends PersistenceHandler {

    private static ExecutorService executorService = Executors.newFixedThreadPool(1);
    private static final Logger LOG = LoggerFactory.getLogger(RedisPersistenceHandler.class);
    private static final String FAILED_BULKS_QUEUE_NAME = "failed_bulks_queue";
    private static final String OVERFLOWED_EVENTS_QUEUE_NAME = "overflowed_events_queue";
    private static final String FAILED_BULK_PREFIX = "failed_bulk#";
    private static final String OVERFLOW_EVENTS_PREFIX = "overflow_events#";
    private static final double TIME_INTERVAL_IN_SECONDS = 600; // 10 minutes

    static final String REDIS_CONFIG = "redis_config";

    private RedisService redisService;

    RedisPersistenceHandler(int maxFetchedBulks, int maxFetchedEvents, int maxInsertTries, RedisServiceConfig redisServiceConfig) {
        super(maxFetchedBulks, maxFetchedEvents, maxInsertTries);
        this.redisService = new RedisService(redisServiceConfig);
        LOG.info("Redis persistence handler is up.");
    }


    @Override
    public List<DbBulkRequest> fetchAndDeleteFailedBulks() {
        if (!shouldFetch(FAILED_BULKS_QUEUE_NAME)) {
            return new ArrayList<>();
        }

        LOG.info("Fetching failed bulks from Redis.");
        List<String> ids = redisService.popRedisSortedSet(FAILED_BULKS_QUEUE_NAME, maxFetchedBulksInOneTime);
        // get matching failed bulks from redis
        Map<String, DbBulkRequest> failedBulkRequests = redisService.getFromRedis(ids);
        // increase times fetched for each fetched one
        failedBulkRequests.values().forEach(dbBulkRequest -> dbBulkRequest.setTimesFetched(dbBulkRequest.getTimesFetched() + 1));
        redisService.deleteFromRedis(ids);

        LOG.info("Number of fetched bulks: {}.", failedBulkRequests.size());
        return new ArrayList<>(failedBulkRequests.values());
    }

    @Override
    public List<Event> fetchAndDeleteOverflowedEvents() {
        if (!shouldFetch(OVERFLOWED_EVENTS_QUEUE_NAME)) {
            return new ArrayList<>();
        }
        LOG.info("Fetching overflowed events from Redis.");
        List<String> ids = redisService.popRedisSortedSet(OVERFLOWED_EVENTS_QUEUE_NAME, maxFetchedEventsListsInOneTime);
        // get matching overflowed events from redis
        Map<String, List<Event>> overflowedEventsLists = redisService.getFromRedis(ids);
        redisService.deleteFromRedis(overflowedEventsLists.keySet());

        List<Event> overflowedEvents = overflowedEventsLists.values().stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        LOG.info("Overflowed events fetch from Redis succeeded. Number of overflowed events: {}.", overflowedEvents.size());
        return overflowedEvents;
    }

    @Override
    public Future<?> persistBulkRequest(DbBulkRequest dbBulkRequest, int bulkNum) {
        return persistBulkRequest(dbBulkRequest, bulkNum, null);
    }

    @Override
    public void persistEvents(ArrayList<Event> events) {
        persistEvents(events, null);
    }

    @Override
    public boolean hasFailedBulks() {
        return redisService.getSortedSetSize(FAILED_BULKS_QUEUE_NAME) > 0;
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
        // including expired failed bulks
        return redisService.getSortedSetSize(FAILED_BULKS_QUEUE_NAME);
    }

    @Override
    public long overFlowedEventsListsAmount() {
        // including expired overflowed events
        return redisService.getSortedSetSize(OVERFLOWED_EVENTS_QUEUE_NAME);
    }

    @Override
    public void close() {
    }

    @Override
    public void reset() {
        List<String> queues = Arrays.asList(OVERFLOWED_EVENTS_QUEUE_NAME, FAILED_BULKS_QUEUE_NAME);
        for (String queue : queues) {
            List<String> ids;
            do {
                ids = redisService.popRedisSortedSet(queue, 100);
                redisService.deleteFromRedis(ids);
            }
            while (ids.size() > 0);
        }
        LOG.info("Finished to reset Redis persistence data");
    }

    Future<?> persistBulkRequest(DbBulkRequest dbBulkRequest, Integer bulkNum, Integer ttl) {
        return executorService.submit(() -> {
            LOG.info("Bulk #{} Pushing bulk request to Redis for the {}th time.", bulkNum, dbBulkRequest.getTimesFetched() + 1);
            Map<String, DbBulkRequest> map = new HashMap<>();
            String key = FAILED_BULK_PREFIX + UUID.randomUUID().toString();
            map.put(key, dbBulkRequest);
            if (!(redisService.pushToRedisSortedSet(FAILED_BULKS_QUEUE_NAME, key, Instant.now().getEpochSecond()) && redisService.pushToRedis(map, ttl))) {
                LOG.error("Failed to persist bulk request number {} to Redis", bulkNum);
            } else {
                LOG.info("Bulk #{} Bulk request was pushed successfully to Redis.", bulkNum);
            }
        });
    }

    void persistEvents(ArrayList<Event> events, Integer ttl) {
        Map<String, ArrayList<Event>> map = new HashMap<>();
        String key = OVERFLOW_EVENTS_PREFIX + UUID.randomUUID().toString();
        map.put(key, events);
        if (!(redisService.pushToRedisSortedSet(OVERFLOWED_EVENTS_QUEUE_NAME, key, Instant.now().getEpochSecond()) && redisService.pushToRedis(map, ttl))) {
            LOG.error("Failed to persist overflowed events list to Redis");
        }  else {
            LOG.info("List of {} overflowed events was pushed successfully to Redis.", events.size());
        }
    }

    private boolean shouldFetch(String queueName) {
        // check that elements in queue are in Redis at list TIME_INTERVAL_IN_SECONDS
        // in order to increase persisted elements lifetime in Redis
        Double minScore = redisService.getMinSocre(queueName);
        if (minScore == null) {
            // queue is empty
            return false;
        } else {
            return Instant.now().getEpochSecond() - minScore >= TIME_INTERVAL_IN_SECONDS;
        }
    }
}

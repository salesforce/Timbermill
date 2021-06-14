package com.datorama.oss.timbermill.common.persistence;

import com.datorama.oss.timbermill.common.redis.RedisService;
import com.datorama.oss.timbermill.unit.Event;
import com.github.jedis.lock.JedisLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final String FAILED_BULKS_LOCK = "failed_bulks_lock";
    private static final String OVERFLOWED_EVENTS_LOCK = "overflowed_events_lock";

    static final String REDIS_SERVICE = "redis_service";
    static final String TTL = "ttl";

    private RedisService redisService;
    private int ttl;

    RedisPersistenceHandler(int maxFetchedBulks, int maxFetchedEvents, int maxInsertTries, int ttl, RedisService redisService) {
        super(maxFetchedBulks, maxFetchedEvents, maxInsertTries);
        this.ttl = ttl;
        if (redisService == null){
            throw new RuntimeException("Redis persistence used but no redis host defined");
        }
        this.redisService = redisService;
        LOG.info("Redis persistence handler is up.");
    }


    @Override
    public List<DbBulkRequest> fetchAndDeleteFailedBulks() {
        JedisLock lock;
        if ((lock = redisService.lockIfUnlocked(FAILED_BULKS_LOCK)) != null) {
            try {
                return fetchAndDeleteFailedBulksLogic();
            } finally {
                redisService.release(lock);
            }
        } else {
            return new ArrayList<>();
        }
    }

    @Override
    public List<Event> fetchAndDeleteOverflowedEvents() {
        JedisLock lock;
        if ((lock = redisService.lockIfUnlocked(OVERFLOWED_EVENTS_LOCK)) != null) {
            try {
                return fetchAndDeleteOverflowedEventsLogic();
            } finally {
                redisService.release(lock);
            }
        } else {
            return new ArrayList<>();
        }
    }

    @Override
    public void persistBulkRequest(DbBulkRequest dbBulkRequest, int bulkNum) {
         persistBulkRequest(dbBulkRequest, bulkNum, ttl);
    }

    @Override
    public void persistEvents(ArrayList<Event> events) {
        persistEvents(events, ttl);
    }

    @Override
    public boolean hasFailedBulks() {
        return redisService.getListLength(FAILED_BULKS_QUEUE_NAME) > 0;
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
        return redisService.getListLength(FAILED_BULKS_QUEUE_NAME);
    }

    @Override
    public long overFlowedEventsListsAmount() {
        // including expired overflowed events
        return redisService.getListLength(OVERFLOWED_EVENTS_QUEUE_NAME);
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
                ids = redisService.popFromRedisList(queue, 100);
                redisService.deleteFromRedis(ids);
            }
            while (ids.size() > 0);
        }
        LOG.info("Finished to reset Redis persistence data");
    }

    private List<DbBulkRequest> fetchAndDeleteFailedBulksLogic() {
        LOG.info("Fetching failed bulks from Redis.");
        List<String> ids = redisService.popFromRedisList(FAILED_BULKS_QUEUE_NAME, maxFetchedBulksInOneTime);
        // get matching failed bulks from redis
        Map<String, DbBulkRequest> failedBulkRequests = redisService.getFromRedis(ids, true);
        // increase times fetched for each fetched one
        failedBulkRequests.values().forEach(dbBulkRequest -> dbBulkRequest.setTimesFetched(dbBulkRequest.getTimesFetched() + 1));
        redisService.deleteFromRedis(ids);

        LOG.info("Number of fetched bulks: {}.", failedBulkRequests.size());
        return new ArrayList<>(failedBulkRequests.values());
    }

    private List<Event> fetchAndDeleteOverflowedEventsLogic() {
        LOG.info("Fetching overflowed events from Redis.");
        List<String> ids = redisService.popFromRedisList(OVERFLOWED_EVENTS_QUEUE_NAME, maxFetchedEventsListsInOneTime);
        // get matching overflowed events from redis
        Map<String, List<Event>> overflowedEventsLists = redisService.getFromRedis(ids, true);
        redisService.deleteFromRedis(overflowedEventsLists.keySet());

        List<Event> overflowedEvents = overflowedEventsLists.values().stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        LOG.info("Overflowed events fetch from Redis succeeded. Number of overflowed events: {}.", overflowedEvents.size());
        return overflowedEvents;
    }

    void persistBulkRequest(DbBulkRequest dbBulkRequest, int bulkNum, int ttl) {
        LOG.info("Bulk #{} Pushing bulk request to Redis for the {}th time.", bulkNum, dbBulkRequest.getTimesFetched() + 1);
        Map<String, DbBulkRequest> map = new HashMap<>();
        String key = FAILED_BULK_PREFIX + UUID.randomUUID().toString();
        map.put(key, dbBulkRequest);
        if (!(redisService.pushToRedisList(FAILED_BULKS_QUEUE_NAME, key) && redisService.pushToRedis(map, ttl))) {
            LOG.error("Failed to persist bulk request number {} to Redis", bulkNum);
        } else {
            LOG.info("Bulk #{} Key {} Bulk request was pushed successfully to Redis.", bulkNum, key);
        }
    }

    void persistEvents(ArrayList<Event> events, int ttl) {
        Map<String, ArrayList<Event>> map = new HashMap<>();
        String key = OVERFLOW_EVENTS_PREFIX + UUID.randomUUID().toString();
        map.put(key, events);
        if (!(redisService.pushToRedisList(OVERFLOWED_EVENTS_QUEUE_NAME, key) && redisService.pushToRedis(map, ttl))) {
            LOG.error("Failed to persist overflowed events list to Redis");
        } else {
            LOG.info("Key {}: List of {} overflowed events was pushed successfully to Redis.", key, events.size());
        }
    }

}

package com.datorama.oss.timbermill.common.persistence;

import com.datorama.oss.timbermill.common.redis.RedisServiceConfig;
import com.datorama.oss.timbermill.common.redis.RedisService;
import com.datorama.oss.timbermill.unit.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class RedisPersistenceHandler extends PersistenceHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RedisPersistenceHandler.class);
    public static final String REDIS_CONFIG = "redis_config";

    private static final String FAILED_BULKS_QUEUE_NAME = "failed_bulks_queue";
    private static final String OVERFLOWED_EVENTS_QUEUE_NAME = "overflowed_events_queue";

    private static final String FAILED_BULK_PREFIX = "failed_bulk#";
    private static final String OVERFLOW_EVENT_PREFIX = "overflow_event#";

    private final RedisService redisService;

    RedisPersistenceHandler(int maxFetchedBulks, int maxFetchedEvents, int maxInsertTries, RedisServiceConfig redisServiceConfig) {
        super(maxFetchedBulks, maxFetchedEvents, maxInsertTries);
        this.redisService = new RedisService(redisServiceConfig);
    }


    @Override
    public List<DbBulkRequest> fetchAndDeleteFailedBulks() {
        List<String> ids = new ArrayList<>();
        Map<String, DbBulkRequest> failedBulkRequests = new HashMap<>();
        do {
            ids.clear();
            failedBulkRequests.clear();

            // pop keys from redis list
            ids.addAll(popElementsFromRedisList(maxFetchedBulksInOneTime));
            // get matching failed bulks from redis
            Map<String, DbBulkRequest> idsToValues = redisService.getFromRedis(ids);
            failedBulkRequests.putAll(idsToValues);
            if (idsToValues.size() < ids.size()) {
                LOG.error("Failed to pull some ids from Redis failed bulks persistence");
            }
        }
        while (areExpiredIds(ids, failedBulkRequests));

        redisService.deleteFromRedis(failedBulkRequests.keySet());
        return new ArrayList<>(failedBulkRequests.values());
    }

    @Override
    public List<Event> fetchAndDeleteOverflowedEvents() {
        List<String> ids = new ArrayList<>();
        Map<String, Event> overflowedEvents = new HashMap<>();
        do {
            ids.clear();
            overflowedEvents.clear();

            // pop keys from redis list
            ids.addAll(popElementsFromRedisList(maxFetchedEventsInOneTime));
            // get matching overflowed events from redis
            Map<String, Event> idsToValues = redisService.getFromRedis(ids);
            overflowedEvents.putAll(idsToValues);
            if (idsToValues.size() < ids.size()) {
                LOG.error("Failed to pull some ids from Redis overflowed events persistence");
            }
        }
        while (areExpiredIds(ids, overflowedEvents));

        redisService.deleteFromRedis(overflowedEvents.keySet());
        return new ArrayList<>(overflowedEvents.values());
    }

    @Override
    public void persistBulkRequest(DbBulkRequest dbBulkRequest, int bulkNum) {
        Map<String, DbBulkRequest> map = new HashMap<>();
        String key = FAILED_BULK_PREFIX + UUID.randomUUID().toString();
        map.put(key, dbBulkRequest);
        if (redisService.pushToRedisList(FAILED_BULKS_QUEUE_NAME, key)){
            redisService.pushToRedisHash(map);
        } else {
            LOG.error("Failed to persist bulk request number {} to Redis", bulkNum);
        }
    }

    @Override
    void persistEvents(ArrayList<Event> events) {

    }

    @Override
    public boolean hasFailedBulks() {
        List<String> ids = new ArrayList<>();
        Map<String, DbBulkRequest> failedBulkRequests = new HashMap<>();
        do {
            ids.clear();
            failedBulkRequests.clear();

            ids.addAll(redisService.getRangeFromRedisList(FAILED_BULKS_QUEUE_NAME,0,100));
            Map<String, DbBulkRequest> idsToValues = redisService.getFromRedis(ids);
            failedBulkRequests.putAll(idsToValues);
            if (idsToValues.size() < ids.size()) {
                LOG.error("Failed to pull some ids from Redis failed bulks persistence");
            }
        }
        while (areExpiredIds(ids, failedBulkRequests));
        return failedBulkRequests.size() > 0;
    }

    @Override
    public boolean isCreatedSuccessfully() {
        return redisService.isConnected();
    }

    @Override
    long failedBulksAmount() {
        List<String> ids = new ArrayList<>();
        int failedBulkRequestsAmount = 0;
        do {
            ids.clear();

            ids.addAll(redisService.getRangeFromRedisList(FAILED_BULKS_QUEUE_NAME,0,100));
            Map<String, DbBulkRequest> idsToValues = redisService.getFromRedis(ids);
            failedBulkRequestsAmount += idsToValues.size();
            if (idsToValues.size() < ids.size()) {
                LOG.error("Failed to pull some ids from Redis failed bulks persistence");
            }
        }
        while (ids.size() > 0);
        return failedBulkRequestsAmount;
    }

    @Override
    long overFlowedEventsAmount() {
        return 0;
    }

    @Override
    public void close() {
        redisService.close();
    }

    private List<String> popElementsFromRedisList(int amount) {
        List<String> elements = redisService.getRangeFromRedisList(FAILED_BULKS_QUEUE_NAME, 0, amount);
        redisService.trimRedisList(FAILED_BULKS_QUEUE_NAME, elements.size(), -1);
        return elements;
    }

    private <T> boolean areExpiredIds(List<String> ids, Map<String, T> failedBulkRequests) {
        return ids.size() > 0 && failedBulkRequests.size() == 0;
    }
}

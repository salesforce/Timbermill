package com.datorama.oss.timbermill.common.persistence;

import com.datorama.oss.timbermill.RedisCacheConfig;
import com.datorama.oss.timbermill.common.cache.RedisCacheHandler;
import com.datorama.oss.timbermill.unit.Event;
import com.datorama.oss.timbermill.unit.LocalTask;
import com.datorama.oss.timbermill.unit.TaskMetaData;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import com.esotericsoftware.kryo.util.Pool;
import com.evanlennick.retry4j.CallExecutorBuilder;
import com.evanlennick.retry4j.Status;
import com.evanlennick.retry4j.config.RetryConfig;
import com.evanlennick.retry4j.config.RetryConfigBuilder;
import com.evanlennick.retry4j.exception.RetriesExhaustedException;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.io.ByteArrayOutputStream;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.Callable;

public class RedisPersistenceHandler extends PersistenceHandler {


    static final String REDIS_CONFIG = "REDIS_CONFIG";

    private static final String FAILED_BULKS_QUEUE_NAME = "failed_bulks_queue";
    private static final String OVERFLOWED_EVENTS_QUEUE_NAME = "overflowed_events_queue";

    private static final String FAILED_BULKS_KEY_NAME = "failed_bulks_key";
    private static final String OVERFLOWED_EVENTs_KEY_NAME = "overflowed_events_key";

    private static final Logger LOG = LoggerFactory.getLogger(RedisCacheHandler.class);

    private final JedisPool jedisPool;
    private final Pool<Kryo> kryoPool;
    private final RetryConfig retryConfig;
    private int redisTtlInSeconds;
    private int redisGetSize;
    private int redisMaxTries;


    RedisPersistenceHandler(int maxFetchedBulks, int maxInsertTries, RedisCacheConfig redisCacheConfig) {
        super(maxFetchedBulks, maxInsertTries);
        redisTtlInSeconds = redisCacheConfig.getRedisTtlInSeconds();
        redisGetSize = redisCacheConfig.getRedisGetSize();
        redisMaxTries = redisCacheConfig.getRedisMaxTries();

        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(redisCacheConfig.getRedisPoolMaxTotal());
        poolConfig.setMinIdle(redisCacheConfig.getRedisPoolMinIdle());
        poolConfig.setMaxIdle(redisCacheConfig.getRedisPoolMaxIdle());
        poolConfig.setTestOnBorrow(true);

        String redisHost = redisCacheConfig.getRedisHost();
        int redisPort = redisCacheConfig.getRedisPort();
        boolean redisUseSsl = redisCacheConfig.isRedisUseSsl();
        String redisPass = redisCacheConfig.getRedisPass();
        if (StringUtils.isEmpty(redisPass)) {
            jedisPool = new JedisPool(poolConfig, redisHost, redisPort, Protocol.DEFAULT_TIMEOUT, redisUseSsl);
        } else {
            jedisPool = new JedisPool(poolConfig, redisHost, redisPort, Protocol.DEFAULT_TIMEOUT, redisPass, redisUseSsl);
        }

        try (Jedis jedis = jedisPool.getResource()) {
            String redisMaxMemory = redisCacheConfig.getRedisMaxMemory();
            if (!StringUtils.isEmpty(redisMaxMemory)) {
                jedis.configSet("maxmemory", redisMaxMemory);
            }
            String redisMaxMemoryPolicy = redisCacheConfig.getRedisMaxMemoryPolicy();
            if (!StringUtils.isEmpty(redisMaxMemoryPolicy)) {
                jedis.configSet("maxmemory-policy", "allkeys-lru");
            }
        }

        kryoPool = new Pool<Kryo>(true, false, 10) {
            protected Kryo create () {
                Kryo kryo = new Kryo();
                kryo.setDefaultSerializer(CompatibleFieldSerializer.class);
                kryo.register(LocalTask.class);
                kryo.register(java.util.HashMap.class);
                kryo.register(java.util.ArrayList.class);
                kryo.register(TaskMetaData.class);
                kryo.register(java.time.ZonedDateTime.class);
                kryo.register(com.datorama.oss.timbermill.unit.TaskStatus.class);
                kryo.register(byte[].class);
                kryo.register(com.datorama.oss.timbermill.common.persistence.DbBulkRequest.class);
                kryo.register(org.elasticsearch.action.bulk.BulkRequest.class, new BulkRequestSerializer());
                return kryo;
            }
        };
        retryConfig = new RetryConfigBuilder()
                .withMaxNumberOfTries(redisMaxTries)
                .retryOnAnyException()
                .withDelayBetweenTries(1, ChronoUnit.SECONDS)
                .withExponentialBackoff()
                .build();
        LOG.info("Connected to Redis");

        persistBulkRequest(new DbBulkRequest(new BulkRequest()),0);
        List<DbBulkRequest> dbBulkRequests = fetchAndDeleteFailedBulks();
        System.out.println(dbBulkRequests.size());
    }


    private <T> Map<String, T> getFromRedis(Collection<String> keys, String cacheName) {
        Map<String, T> retMap = Maps.newHashMap();
        for (List<String> idsPartition : Iterables.partition(keys, redisGetSize)) {
            byte[][] ids = new byte[idsPartition.size()][];
            for (int i = 0; i < idsPartition.size(); i++) {
                ids[i] = idsPartition.get(i).getBytes();
            }
            try (Jedis jedis = jedisPool.getResource()) {
                List<byte[]> serializedObjects = runWithRetries(() -> jedis.mget(ids), "MGET Keys");
                for (int i = 0; i < ids.length; i++) {
                    byte[] serializedObject = serializedObjects.get(i);

                    if (serializedObject == null || serializedObject.length == 0) {
                        continue;
                    }
                    Kryo kryo  = kryoPool.obtain();
                    try {
                        T object = (T) kryo.readClassAndObject(new Input(serializedObject));
                        String id = new String(ids[i]);
                        retMap.put(id, object);
                    }  catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                    finally {
                        kryoPool.free(kryo);
                    }

                }
            } catch (Exception e) {
                LOG.error("Error getting ids from Redis " + cacheName + " cache. Ids: " + idsPartition , e);
            }
        }
        return retMap;
    }

    private void deleteFromRedis(Collection<String> keys) {
        try (Jedis jedis = jedisPool.getResource(); Pipeline pipelined = jedis.pipelined()) {
            for (String key : keys) {
                try {
                    pipelined.del(key);
                } catch (Exception e) {
                    LOG.error("Error deleting key " + key + " from Redis.", e);
                }
            }
        }
    }

    private <T> void pushToRedisHash(Map<String, T> idsToValuesMap, String cacheName) {
        try (Jedis jedis = jedisPool.getResource(); Pipeline pipelined = jedis.pipelined()) {
            for (Map.Entry<String, T> entry : idsToValuesMap.entrySet()) {
                String id = entry.getKey();
                T object = entry.getValue();
                byte[] taskByteArr = getBytes(object);

                try {
                    pipelined.setex(id.getBytes(), redisTtlInSeconds, taskByteArr);
                } catch (Exception e) {
                    LOG.error("Error pushing id " + id + " to Redis " + cacheName + " cache", e);
                }
            }
        }
    }

    private void pushToRedisList(String listName, String value) {
        try (Jedis jedis = jedisPool.getResource(); Pipeline pipelined = jedis.pipelined()) {
            try {
                pipelined.lpush(listName, value);
            } catch (Exception e) {
                LOG.error("Error pushing item to Redis " + listName + " list", e);
            }
        }
    }

    private List<String> getRangeFromRedisList(String listName, int start, int end) {
        List<String> ids;
        try (Jedis jedis = jedisPool.getResource()) {
            ids = runWithRetries(() -> jedis.lrange(listName, start, end), "LRANGE");
        } catch (Exception e) {
            LOG.error("Error getting elements from Redis " + listName + " list", e);
            ids = new ArrayList<>();
        }
        return ids;
    }

    private void trimRedisList(String listName, int start, int end) {
        try (Jedis jedis = jedisPool.getResource()) {
            runWithRetries(() -> jedis.ltrim(listName, start, end), "TRIM");
        } catch (Exception e) {
            LOG.error("Error trimming Redis " + listName + " list", e);
        }
    }

    private byte[] getBytes(Object orphansIds) {
        ByteArrayOutputStream objStream = new ByteArrayOutputStream();
        Output objOutput = new Output(objStream);

        Kryo kryo = kryoPool.obtain();
        try {
            kryo.writeClassAndObject(objOutput, orphansIds);
            objOutput.close();
            return objStream.toByteArray();
        } finally {
            kryoPool.free(kryo);
        }
    }

    private <T> T runWithRetries(Callable<T> callable, String functionDescription) throws RetriesExhaustedException {
        Status<T> status = new CallExecutorBuilder<T>()
                .config(retryConfig)
                .onFailureListener(this::printFailWarning)
                .build()
                .execute(callable, functionDescription);
        return status.getResult();
    }

    private void printFailWarning(Status status) {
        LOG.warn("Failed try # " + status.getTotalTries() + "/" + redisMaxTries + " for [Redis - " + status.getCallName() + "] ", status.getLastExceptionThatCausedRetry());
    }



    // -------------------------------------------------------------------------------

    @Override
    public List<DbBulkRequest> fetchAndDeleteFailedBulks() {
        List<String> ids = new ArrayList<>();
        Map<String, DbBulkRequest> failedBulkRequests = new HashMap<>();
        do {
            ids.clear();
            failedBulkRequests.clear();

            ids.addAll(getRangeFromRedisList(FAILED_BULKS_QUEUE_NAME, 0, maxFetchedBulksInOneTime));
            trimRedisList(FAILED_BULKS_QUEUE_NAME, ids.size(), -1);
            failedBulkRequests.putAll(getFromRedis(ids, FAILED_BULKS_KEY_NAME));
        }
        while (ids.size() != 0 && failedBulkRequests.size() == 0);
        deleteFromRedis(failedBulkRequests.keySet());
        return new ArrayList<>(failedBulkRequests.values());
    }

    @Override
    public List<Event> fetchAndDeleteOverflowedEvents() {
        return null;
    }

    @Override
    public void persistBulkRequest(DbBulkRequest dbBulkRequest, int bulkNum) {
        Map<String, DbBulkRequest> map = new HashMap<>();
        String key = UUID.randomUUID().toString();
        map.put(key, dbBulkRequest);
        pushToRedisHash(map, FAILED_BULKS_KEY_NAME);
        pushToRedisList(FAILED_BULKS_QUEUE_NAME, key);
    }

    @Override
    void persistEvents(ArrayList<Event> events) {

    }

    @Override
    public boolean hasFailedBulks() {
        return false;
    }

    @Override
    public boolean isCreatedSuccessfully() {
        return false;
    }

    @Override
    long failedBulksAmount() {
        return 0;
    }

    @Override
    long overFlowedEventsAmount() {
        return 0;
    }

    @Override
    public void close() {

    }
}

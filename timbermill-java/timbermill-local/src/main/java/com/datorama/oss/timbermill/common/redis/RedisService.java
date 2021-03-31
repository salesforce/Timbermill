package com.datorama.oss.timbermill.common.redis;

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
import com.github.jedis.lock.JedisLock;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.io.ByteArrayOutputStream;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class RedisService {

    private static final String LOCK_NAME = "timbermill__lock";
    private static final Logger LOG = LoggerFactory.getLogger(RedisService.class);

    private final JedisPool jedisPool;
    private final Pool<Kryo> kryoPool;
    private JedisLock lock;
    private final RetryConfig retryConfig;
    private int redisTtlInSeconds;
    private int redisGetSize;
    private int redisMaxTries;

    public RedisService(RedisServiceConfig redisServiceConfig) {
        redisTtlInSeconds = redisServiceConfig.getRedisTtlInSeconds();
        redisGetSize = redisServiceConfig.getRedisGetSize();
        redisMaxTries = redisServiceConfig.getRedisMaxTries();

        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(redisServiceConfig.getRedisPoolMaxTotal());
        poolConfig.setMinIdle(redisServiceConfig.getRedisPoolMinIdle());
        poolConfig.setMaxIdle(redisServiceConfig.getRedisPoolMaxIdle());
        poolConfig.setTestOnBorrow(true);

        String redisHost = redisServiceConfig.getRedisHost();
        int redisPort = redisServiceConfig.getRedisPort();
        boolean redisUseSsl = redisServiceConfig.isRedisUseSsl();
        String redisPass = redisServiceConfig.getRedisPass();
        if (StringUtils.isEmpty(redisPass)) {
            jedisPool = new JedisPool(poolConfig, redisHost, redisPort, Protocol.DEFAULT_TIMEOUT, redisUseSsl);
        } else {
            jedisPool = new JedisPool(poolConfig, redisHost, redisPort, Protocol.DEFAULT_TIMEOUT, redisPass, redisUseSsl);
        }

        try (Jedis jedis = jedisPool.getResource()) {
            String redisMaxMemory = redisServiceConfig.getRedisMaxMemory();
            if (!StringUtils.isEmpty(redisMaxMemory)) {
                jedis.configSet("maxmemory", redisMaxMemory);
            }
            String redisMaxMemoryPolicy = redisServiceConfig.getRedisMaxMemoryPolicy();
            if (!StringUtils.isEmpty(redisMaxMemoryPolicy)) {
                jedis.configSet("maxmemory-policy", "allkeys-lru");
            }
        }

        kryoPool = new Pool<Kryo>(true, false, 10) {
            protected Kryo create() {
                Kryo kryo = new Kryo();
                kryo.setDefaultSerializer(CompatibleFieldSerializer.class);
                kryo.register(LocalTask.class);
                kryo.register(java.util.HashMap.class);
                kryo.register(java.util.ArrayList.class);
                kryo.register(TaskMetaData.class);
                kryo.register(java.time.ZonedDateTime.class);
                kryo.register(com.datorama.oss.timbermill.unit.TaskStatus.class);
                kryo.register(com.datorama.oss.timbermill.unit.SpotEvent.class);
                kryo.register(com.datorama.oss.timbermill.unit.InfoEvent.class);
                kryo.register(com.datorama.oss.timbermill.unit.SuccessEvent.class);
                kryo.register(com.datorama.oss.timbermill.unit.ErrorEvent.class);
                kryo.register(com.datorama.oss.timbermill.unit.StartEvent.class);
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
    }

    // region public methods

    public <T> Map<String, T> getFromRedis(Collection<String> keys) {
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
                    Kryo kryo = kryoPool.obtain();
                    try {
                        T object = (T) kryo.readClassAndObject(new Input(serializedObject));
                        String id = new String(ids[i]);
                        retMap.put(id, object);
                    } catch (Exception e) {
                        LOG.error("Error getting ids from Redis. Ids: " + idsPartition, e);
                    } finally {
                        kryoPool.free(kryo);
                    }

                }
            } catch (Exception e) {
                LOG.error("Error getting ids from Redis. Ids: " + idsPartition, e);
            }
        }
        return retMap;
    }

    public void deleteFromRedis(Collection<String> keys) {
        try (Jedis jedis = jedisPool.getResource(); Pipeline pipelined = jedis.pipelined()) {
            for (String key : keys) {
                try {
                    runWithRetries(() -> pipelined.del(key), "DEL");
                } catch (Exception e) {
                    LOG.error("Error deleting key " + key + " from Redis.", e);
                }
            }
        }
    }

    public <T> boolean pushToRedisHash(Map<String, T> idsToValuesMap, int redisTtlInSeconds) {
        boolean allPushed = true;
        try (Jedis jedis = jedisPool.getResource(); Pipeline pipelined = jedis.pipelined()) {
            for (Map.Entry<String, T> entry : idsToValuesMap.entrySet()) {
                String id = entry.getKey();
                T object = entry.getValue();

                try {
                    byte[] taskByteArr = getBytes(object);
                    runWithRetries(() -> pipelined.setex(id.getBytes(), redisTtlInSeconds, taskByteArr), "SETEX");
                } catch (Exception e) {
                    allPushed = false;
                    LOG.error("Error pushing id " + id + " to Redis.", e);
                }
            }
        }
        return allPushed;
    }

    public <T> boolean pushToRedisHash(Map<String, T> idsToValuesMap) {
        return pushToRedisHash(idsToValuesMap, redisTtlInSeconds);
    }

    public <T> boolean pushToRedisList(String listName, T value) {
        boolean allPushed = true;
        try (Jedis jedis = jedisPool.getResource()) {
            try {
                byte[] valueByteArr = getBytes(value);
                runWithRetries(() -> jedis.rpush(listName.getBytes(), valueByteArr), "RPUSH");
            } catch (Exception e) {
                allPushed = false;
                LOG.error("Error pushing item to Redis " + listName + " list", e);
            }
        }
        return allPushed;
    }

    public long getListLength(String listName) {
        try (Jedis jedis = jedisPool.getResource()) {
            try {
                return runWithRetries(() -> jedis.llen(listName.getBytes()), "LLEN").longValue();
            } catch (Exception e) {
                LOG.error("Error returning Redis " + listName + " list length", e);
                return -1;
            }
        }
    }

    public <T> List<T> getRangeFromRedisList(String listName, int start, int end) {
        List<T> ids = new ArrayList<>();
        try (Jedis jedis = jedisPool.getResource()) {
            List<byte[]> serializedObjects = runWithRetries(() -> jedis.lrange(listName.getBytes(), start, end), "LRANGE");
            for (int i = 0; i < serializedObjects.size(); i++) {
                byte[] serializedObject = serializedObjects.get(i);

                if (serializedObject == null || serializedObject.length == 0) {
                    continue;
                }
                Kryo kryo = kryoPool.obtain();
                try {
                    T object = (T) kryo.readClassAndObject(new Input(serializedObject));
                    ids.add(object);
                } catch (Exception e) {
                    LOG.error("Error getting elements from Redis " + listName + " list", e);
                } finally {
                    kryoPool.free(kryo);
                }

            }
        } catch (Exception e) {
            LOG.error("Error getting elements from Redis " + listName + " list", e);
            ids = new ArrayList<>();
        }
        return ids;
    }

    public void trimRedisList(String listName, int start, int end) {
        try (Jedis jedis = jedisPool.getResource()) {
            runWithRetries(() -> jedis.ltrim(listName, start, end), "TRIM");
        } catch (Exception e) {
            LOG.error("Error trimming Redis " + listName + " list", e);
        }
    }

    public void lock() {
        lock = new JedisLock(LOCK_NAME, 20000, 20000);
        try (Jedis jedis = jedisPool.getResource()) {
            lock.acquire(jedis);
        } catch (Exception e) {
            LOG.error("Error while locking lock in Redis", e);
        }
    }

    public void release() {
        try (Jedis jedis = jedisPool.getResource()) {
            lock.release(jedis);
        } catch (Exception e) {
            LOG.error("Error while releasing lock in Redis", e);
        }
    }

    public void close() {
        jedisPool.close();
    }

    public boolean isConnected() {
        return !jedisPool.isClosed();
    }

    // endregion


    // region private methods

    private byte[] getBytes(Object object) {
        ByteArrayOutputStream objStream = new ByteArrayOutputStream();
        Output objOutput = new Output(objStream);

        Kryo kryo = kryoPool.obtain();
        try {
            kryo.writeClassAndObject(objOutput, object);
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

    // endregion

}
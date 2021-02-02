package com.datorama.oss.timbermill.common.cache;

import com.datorama.oss.timbermill.unit.LocalTask;
import com.datorama.oss.timbermill.unit.TaskMetaData;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.io.ByteArrayOutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RedisCacheHandler extends AbstractCacheHandler {

    private static final String ORPHAN_PREFIX = "orphan###";
    private final JedisPool jedisPool;
    private Kryo kryo;
    private int redisTtlInSeconds;

    private static final Logger LOG = LoggerFactory.getLogger(RedisCacheHandler.class);
    private int redisGetSize;


    RedisCacheHandler(String redisHost, int redisPort, String redisPass,
                      String redisMaxMemory, String redisMaxMemoryPolicy, boolean redisUseSsl, int redisTtlInSeconds, int redisGetSize, int redisPoolMinIdle, int redisPoolMaxTotal) {
        this.redisTtlInSeconds = redisTtlInSeconds;
        this.redisGetSize = redisGetSize;

        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(redisPoolMaxTotal);
        poolConfig.setMinIdle(redisPoolMinIdle);
        if (StringUtils.isEmpty(redisPass)) {
            jedisPool = new JedisPool(poolConfig, redisHost, redisPort, Protocol.DEFAULT_TIMEOUT, redisUseSsl);
        } else {
            jedisPool = new JedisPool(poolConfig, redisHost, redisPort, Protocol.DEFAULT_TIMEOUT, redisPass, redisUseSsl);
        }

        try (Jedis jedis = jedisPool.getResource()) {
            if (!StringUtils.isEmpty(redisMaxMemory)) {
                jedis.configSet("maxmemory", redisMaxMemory);
            }
            if (!StringUtils.isEmpty(redisMaxMemoryPolicy)) {
                jedis.configSet("maxmemory-policy", "allkeys-lru");
            }
        }

        kryo = new Kryo();
        kryo.register(LocalTask.class);
        kryo.register(java.util.HashMap.class);
        kryo.register(java.util.ArrayList.class);
        kryo.register(TaskMetaData.class);
        kryo.register(java.time.ZonedDateTime.class);
        kryo.register(com.datorama.oss.timbermill.unit.TaskStatus.class);
        LOG.info("Connected to Redis");
    }

    @Override
    public Map<String, List<String>> pullFromOrphansCache(Collection<String> parentsIds) {
        Set<String> orphanParentsIds = parentsIds.stream().map(s -> ORPHAN_PREFIX + s).collect(Collectors.toSet());
        Map<String, List<String>> orphans = getFromRedis(orphanParentsIds, "orphans");

        Map<String, List<String>> retMap = Maps.newHashMap();
        for (Map.Entry<String, List<String>> entry : orphans.entrySet()) {
            String newKey = entry.getKey().substring(ORPHAN_PREFIX.length());
            retMap.put(newKey, entry.getValue());
        }

        return retMap;
    }

    @Override
    public void pushToOrphanCache(Map<String, List<String>> orphansMap) {
        Map<String, List<String>> newOrphansMap = Maps.newHashMap();
        for (Map.Entry<String, List<String>> entry : orphansMap.entrySet()) {
            String orphanCacheKey = ORPHAN_PREFIX + entry.getKey();
            newOrphansMap.put(orphanCacheKey, entry.getValue());
        }
        pushToRedis(newOrphansMap, "orphans");
    }

    @Override
    public Map<String, LocalTask> getFromTasksCache(Collection<String> idsList) {
        return getFromRedis(idsList, "tasks");
    }

    private <T> Map<String, T> getFromRedis(Collection<String> keys, String cacheName) {
        Map<String, T> retMap = Maps.newHashMap();
        for (List<String> idsPartition : Iterables.partition(keys, redisGetSize)) {
            byte[][] ids = new byte[idsPartition.size()][];
            for (int i = 0; i < idsPartition.size(); i++) {
                ids[i] = idsPartition.get(i).getBytes();
            }
            try (Jedis jedis = jedisPool.getResource()) {
                List<byte[]> serializedObjects = jedis.mget(ids);
                for (int i = 0; i < ids.length; i++) {
                    byte[] serializedObject = serializedObjects.get(i);

                    if (serializedObject == null || serializedObject.length == 0) {
                        continue;
                    }
                    T object = (T) kryo.readClassAndObject(new Input(serializedObject));

                    String id = new String(ids[i]);
                    retMap.put(id, object);
                }
            } catch (Exception e) {
                LOG.error("Error getting ids: " + idsPartition + " from Redis " + cacheName + " cache", e);
            }
        }
        return retMap;
    }

    @Override
    public void pushToTasksCache(Map<String, LocalTask> idsToMap) {
        pushToRedis(idsToMap, "tasks");
    }

    private <T> void pushToRedis(Map<String, T> idsToValuesMap, String cacheName) {
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

    @Override
    public void close() {
        jedisPool.close();
    }

    private byte[] getBytes(Object orphansIds) {
        ByteArrayOutputStream objStream = new ByteArrayOutputStream();
        Output objOutput = new Output(objStream);
        kryo.writeClassAndObject(objOutput, orphansIds);
        objOutput.close();
        return objStream.toByteArray();
    }
}

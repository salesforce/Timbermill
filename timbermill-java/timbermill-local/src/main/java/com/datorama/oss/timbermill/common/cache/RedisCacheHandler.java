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
                      String redisMaxMemory, String redisMaxMemoryPolicy, boolean redisUseSsl, int redisTtlInSeconds, int redisGetSize) {
        this.redisTtlInSeconds = redisTtlInSeconds;
        this.redisGetSize = redisGetSize;

        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(5);
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
    public Map<String, List<String>> pullFromOrphansCache(Set<String> parentsIds) {
        Set<String> orphanParentsIds = parentsIds.stream().map(s -> ORPHAN_PREFIX + s).collect(Collectors.toSet());
        Map<String, List<String>> retMap = Maps.newHashMap();
        for (List<String> idsPartition : Iterables.partition(orphanParentsIds, redisGetSize)) {
            byte[][] ids = new byte[idsPartition.size()][];
            for (int i = 0; i < idsPartition.size(); i++) {
                ids[i] = idsPartition.get(i).getBytes();
            }
            try (Jedis jedis = jedisPool.getResource()) {
                List<byte[]> serializedOrphansIds = jedis.mget(ids);
                jedis.del(ids);
                for (int i = 0; i < ids.length; i++) {
                    byte[] serializedOrphansId = serializedOrphansIds.get(i);

                    if (serializedOrphansId == null || serializedOrphansId.length == 0) {
                        continue;
                    }
                    List<String> orphanList = (List<String>) kryo.readClassAndObject(new Input(serializedOrphansId));

                    String id = new String(ids[i]);
                    id = id.substring(ORPHAN_PREFIX.length());
                    retMap.put(id, orphanList);
                }
            } catch (Exception e) {
                LOG.error("Error getting ids: " + idsPartition + " from Redis tasks' cache", e);
            }
        }
        return retMap;
    }

    @Override
    public void pushToOrphanCache(Map<String, List<String>> orphansMap) {
        try (Jedis jedis = jedisPool.getResource(); Pipeline pipelined = jedis.pipelined()) {
            for (Map.Entry<String, List<String>> entry : orphansMap.entrySet()) {
                String id = ORPHAN_PREFIX + entry.getKey();
                List<String> orphansIds = entry.getValue();


                ByteArrayOutputStream objStream = new ByteArrayOutputStream();
                Output objOutput = new Output(objStream);
                kryo.writeClassAndObject(objOutput, orphansIds);
                objOutput.close();
                byte[] orphanIdsBytes = objStream.toByteArray();


                try {
                    pipelined.setex(id.getBytes(), redisTtlInSeconds, orphanIdsBytes);
                } catch (Exception e) {
                    LOG.error("Error pushing id " + id + " to Redis tasks' cache", e);
                }
            }
        }
    }

    @Override
    public Map<String, LocalTask> getFromTasksCache(Collection<String> idsList) {
        Map<String, LocalTask> retMap = Maps.newHashMap();
        for (List<String> idsPartition : Iterables.partition(idsList, redisGetSize)) {
            byte[][] ids = new byte[idsPartition.size()][];
            for (int i = 0; i < idsPartition.size(); i++) {
                ids[i] = idsPartition.get(i).getBytes();
            }
            try (Jedis jedis = jedisPool.getResource()) {
                List<byte[]> serializedTasks = jedis.mget(ids);
                for (int i = 0; i < ids.length; i++) {
                    String id = new String(ids[i]);
                    byte[] serializedTask = serializedTasks.get(i);

                    if (serializedTask == null || serializedTask.length == 0) {
                        continue;
                    }
                    LocalTask localTask = (LocalTask) kryo.readClassAndObject(new Input(serializedTask));

                    retMap.put(id, localTask);
                }
            } catch (Exception e) {
                LOG.error("Error getting ids: " + idsPartition + " from Redis tasks' cache", e);
            }
        }
        return retMap;
    }

    @Override
    public void pushToTasksCache(Map<String, LocalTask> idsToMap) {
        try (Jedis jedis = jedisPool.getResource(); Pipeline pipelined = jedis.pipelined()) {
            for (Map.Entry<String, LocalTask> entry : idsToMap.entrySet()) {
                String id = entry.getKey();
                LocalTask localTask = entry.getValue();


                ByteArrayOutputStream objStream = new ByteArrayOutputStream();
                Output objOutput = new Output(objStream);
                kryo.writeClassAndObject(objOutput, localTask);
                objOutput.close();
                byte[] taskByteArr = objStream.toByteArray();


                try {
                    pipelined.setex(id.getBytes(), redisTtlInSeconds, taskByteArr);
                } catch (Exception e) {
                    LOG.error("Error pushing id " + id + " to Redis tasks' cache", e);
                }
            }
        }
    }

    @Override
    public void close() {
        jedisPool.close();
    }
}

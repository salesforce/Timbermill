package com.datorama.oss.timbermill.common.cache;

import com.datorama.oss.timbermill.unit.LocalTask;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.datorama.oss.timbermill.ElasticsearchClient.GSON;

public class RedisCacheHandler extends AbstractCacheHandler {

    private final Jedis jedis;
    private int redisTtlInSeconds;

    private static final Logger LOG = LoggerFactory.getLogger(RedisCacheHandler.class);
    private int redisGetSize;


    RedisCacheHandler(long maximumOrphansCacheWeight, String redisHost, int redisPort, String redisPass,
                      String redisMaxMemory, String redisMaxMemoryPolicy, boolean redisUseSsl, int redisTtlInSeconds, int redisGetSize) {
        super(maximumOrphansCacheWeight);
        this.redisTtlInSeconds = redisTtlInSeconds;
        this.redisGetSize = redisGetSize;

        jedis = new Jedis(redisHost, redisPort, redisUseSsl);
        if (!StringUtils.isEmpty(redisPass)){
            jedis.auth(redisPass);
        }
        if (!StringUtils.isEmpty(redisMaxMemory)){
            jedis.configSet("maxmemory", redisMaxMemory);
        }
        if (!StringUtils.isEmpty(redisMaxMemoryPolicy)){
            jedis.configSet("maxmemory-policy", "allkeys-lru");
        }
        LOG.info("Connected to Redis");
    }

    @Override
    public Map<String, LocalTask> getFromTasksCache(Collection<String> idsList) {
        Map<String, LocalTask> retMap = Maps.newHashMap();
        for (List<String> idsPartition : Iterables.partition(idsList, redisGetSize)) {
            String[] ids = idsPartition.toArray(new String[0]);
            try {
                List<String> tasksStrings = jedis.mget(ids);
                for (int i = 0; i < ids.length; i++) {
                    String id = ids[i];
                    String taskString = tasksStrings.get(i);
                    LocalTask localTask = GSON.fromJson(taskString, LocalTask.class);
                    retMap.put(id, localTask);
                }
            } catch (Exception e){
                LOG.error("Error getting ids: " + idsPartition + " from Redis tasks' cache", e);
            }
        }
        return retMap;
    }

    @Override
    public void pushToTasksCache(Map<String, LocalTask> idsToMap) {
        Pipeline pipelined = null;
        try {
            pipelined = jedis.pipelined();
            for (Map.Entry<String, LocalTask> entry : idsToMap.entrySet()) {
                String id = entry.getKey();
                LocalTask localTask = entry.getValue();
                String taskString = GSON.toJson(localTask);
                try {
                    pipelined.setex(id, redisTtlInSeconds, taskString);
                } catch (Exception e) {
                    LOG.error("Error pushing id " + id + " with value " + taskString + " to Redis tasks' cache", e);
                }
            }
        }
        finally {
            if (pipelined != null) {
                pipelined.sync();
            }
        }
    }
}

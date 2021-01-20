package com.datorama.oss.timbermill.common.cache;

import com.datorama.oss.timbermill.unit.LocalTask;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;

import static com.datorama.oss.timbermill.ElasticsearchClient.GSON;

public class RedisCacheHandler extends AbstractCacheHandler {

    private final Jedis jedis;

    RedisCacheHandler(long maximumOrphansCacheWeight, String redisHost, int redisPort, String redisPass, String redisMaxMemory) {
        super(maximumOrphansCacheWeight);
        jedis = new Jedis(redisHost, redisPort);
        if (!StringUtils.isEmpty(redisPass)){
            jedis.auth(redisPass);
        }
        jedis.configSet("maxmemory", redisMaxMemory);
        jedis.configSet("maxmemory-policy", "allkeys-lru");
    }

    @Override
    public LocalTask getFromTasksCache(String id) {
        String taskString = jedis.get(id);
        return GSON.fromJson(taskString, LocalTask.class);
    }

    @Override
    public void pushToTasksCache(String id, LocalTask localTask) {
        String taskString = GSON.toJson(localTask);
        jedis.set(id, taskString);
    }
}

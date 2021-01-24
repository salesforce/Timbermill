package com.datorama.oss.timbermill.common.cache;

import com.datorama.oss.timbermill.unit.LocalTask;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.datorama.oss.timbermill.ElasticsearchClient.GSON;

public class RedisCacheHandler extends AbstractCacheHandler {

    private final Jedis jedis;
    private static final Logger LOG = LoggerFactory.getLogger(RedisCacheHandler.class);


    RedisCacheHandler(long maximumOrphansCacheWeight, String redisHost, int redisPort, String redisPass, String redisMaxMemory) {
        super(maximumOrphansCacheWeight);

        LOG.info("host {}", redisHost);
        LOG.info("redisPort {}", redisPort);
        LOG.info("redisPass {}", redisPass.substring(0, 5));
        jedis = new Jedis(redisHost, redisPort, false);
        if (!StringUtils.isEmpty(redisPass)){
            jedis.auth(redisPass);
        }
//        jedis.configSet("maxmemory", redisMaxMemory);
        jedis.configSet("maxmemory-policy", "allkeys-lru");
    }

    @Override
    public Map<String, LocalTask> getFromTasksCache(Collection<String> idsList) {
        String[] ids = idsList.toArray(new String[0]);
        List<String> tasksStrings = jedis.mget(ids);
        Map<String, LocalTask> retMap = Maps.newHashMap();
        for (int i = 0; i < ids.length; i++) {
            String id = ids[i];
            String taskString = tasksStrings.get(i);
            LocalTask localTask = GSON.fromJson(taskString, LocalTask.class);
            retMap.put(id, localTask);
        }
        return retMap;
    }

    @Override
    public void pushToTasksCache(Map<String, LocalTask> idsToMap) {
        List<String> push = Lists.newArrayList();
        for (Map.Entry<String, LocalTask> entry : idsToMap.entrySet()) {
            String id = entry.getKey();
            LocalTask localTask = entry.getValue();
            String taskString = GSON.toJson(localTask);
            push.add(id);
            push.add(taskString);
        }
        String[] pushArr = push.toArray(new String[0]);
        jedis.mset(pushArr);
    }
}

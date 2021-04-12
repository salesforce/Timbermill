package com.datorama.oss.timbermill.common.cache;

import com.datorama.oss.timbermill.common.redis.RedisService;
import com.datorama.oss.timbermill.common.redis.RedisServiceConfig;
import com.datorama.oss.timbermill.unit.LocalTask;
import com.github.jedis.lock.JedisLock;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RedisCacheHandler extends AbstractCacheHandler {

    private static final String LOCK_NAME = "cache_lock";
    private static final String ORPHAN_PREFIX = "orphan###";
    private static final Logger LOG = LoggerFactory.getLogger(RedisCacheHandler.class);

    private JedisLock lock;
    private final RedisService redisService;


    RedisCacheHandler(RedisServiceConfig redisServiceConfig) {
        this.redisService = new RedisService(redisServiceConfig);
    }

    @Override
    public Map<String, List<String>> pullFromOrphansCache(Collection<String> parentsIds) {
        Set<String> orphanParentsIds = parentsIds.stream().map(s -> ORPHAN_PREFIX + s).collect(Collectors.toSet());
        Map<String, List<String>> orphans = redisService.getFromRedis(orphanParentsIds);

        Map<String, List<String>> retMap = Maps.newHashMap();
        for (Map.Entry<String, List<String>> entry : orphans.entrySet()) {
            String newKey = entry.getKey().substring(ORPHAN_PREFIX.length());
            retMap.put(newKey, entry.getValue());
        }
        if (retMap.size() < orphanParentsIds.size()){
//            LOG.warn("Failed to pull some ids from Redis orphans cache. Ids: {}", Sets.difference(orphanParentsIds, retMap.keySet()));
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
        if (!redisService.pushToRedis(newOrphansMap)){
            LOG.error("Failed to push some ids to Redis orphans cache.");
        }
    }

    @Override
    public Map<String, LocalTask> getFromTasksCache(Collection<String> idsList) {
        Map<String, LocalTask> retMap = redisService.getFromRedis(idsList);
        if (retMap.size() < idsList.size()){
//            LOG.warn("Failed to pull some ids from Redis tasks cache. Ids: {}", Sets.difference(Sets.newHashSet(idsList), retMap.keySet()));
        }
        return retMap;
    }

    @Override
    public void pushToTasksCache(Map<String, LocalTask> idsToMap) {
        boolean allPushed = redisService.pushToRedis(idsToMap);
        if (!allPushed){
            LOG.error("Failed to push some ids to Redis tasks cache.");
        }
    }

    @Override
    public void lock() {
        lock = redisService.lock(LOCK_NAME);
    }

    @Override
    public void release() {
        redisService.release(lock);
    }

    @Override
    public void close() {
        redisService.close();
    }
}

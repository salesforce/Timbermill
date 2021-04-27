package com.datorama.oss.timbermill.common.cache;

import com.datorama.oss.timbermill.common.redis.RedisService;

public class CacheConfig {
    private RedisService redisService;
    private int cacheRedisTtlInSeconds;
    private long maximumTasksCacheWeight;
    private long maximumOrphansCacheWeight;

    public CacheConfig(RedisService redisService, int cacheRedisTtlInSeconds, long maximumTasksCacheWeight, long maximumOrphansCacheWeight) {
        this.redisService = redisService;
        this.cacheRedisTtlInSeconds = cacheRedisTtlInSeconds;
        this.maximumTasksCacheWeight = maximumTasksCacheWeight;
        this.maximumOrphansCacheWeight = maximumOrphansCacheWeight;
    }

    RedisService getRedisService() {
        return redisService;
    }

    int getCacheRedisTtlInSeconds() {
        return cacheRedisTtlInSeconds;
    }

    long getMaximumTasksCacheWeight() {
        return maximumTasksCacheWeight;
    }

    long getMaximumOrphansCacheWeight() {
        return maximumOrphansCacheWeight;
    }

}

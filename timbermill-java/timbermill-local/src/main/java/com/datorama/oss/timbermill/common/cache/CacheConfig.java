package com.datorama.oss.timbermill.common.cache;

import com.datorama.oss.timbermill.common.redis.RedisService;

public class CacheConfig {
    private RedisService redisService;
    private int cacheRedisTtlInSeconds;
    private int orphansCacheRedisTtlInSeconds;
    private int eventsCacheRedisTtlInSeconds;
    private long maximumTasksCacheWeight;
    private long maximumOrphansCacheWeight;

    public CacheConfig(RedisService redisService, int cacheRedisTtlInSeconds, long maximumTasksCacheWeight, long maximumOrphansCacheWeight) {
        this(redisService, cacheRedisTtlInSeconds, maximumTasksCacheWeight, maximumOrphansCacheWeight, cacheRedisTtlInSeconds, cacheRedisTtlInSeconds);
    }
    public CacheConfig(RedisService redisService, int cacheRedisTtlInSeconds, long maximumTasksCacheWeight, long maximumOrphansCacheWeight, int orphansCacheRedisTtlInSeconds,
                       int eventsCacheRedisTtlInSeconds) {
        this.redisService = redisService;
        this.cacheRedisTtlInSeconds = cacheRedisTtlInSeconds;
        this.maximumTasksCacheWeight = maximumTasksCacheWeight;
        this.maximumOrphansCacheWeight = maximumOrphansCacheWeight;
        this.eventsCacheRedisTtlInSeconds = eventsCacheRedisTtlInSeconds;
        this.orphansCacheRedisTtlInSeconds = orphansCacheRedisTtlInSeconds;

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

    public int getOrphansCacheRedisTtlInSeconds() {
        return orphansCacheRedisTtlInSeconds;
    }

    public int getEventsCacheRedisTtlInSeconds() {
        return eventsCacheRedisTtlInSeconds;
    }

}

package com.datorama.oss.timbermill;


import com.datorama.oss.timbermill.common.redis.RedisService;

public class RedisCacheConfig {
    private final RedisService redisService;
    private final int redisTtlInSeconds;

    public RedisCacheConfig(RedisService redisService, int redisTtlInSeconds) {
        this.redisService = redisService;
        this.redisTtlInSeconds = redisTtlInSeconds;
    }

    public RedisService getRedisService() {
        return redisService;
    }

    public int getRedisTtlInSeconds() {
        return redisTtlInSeconds;
    }
}

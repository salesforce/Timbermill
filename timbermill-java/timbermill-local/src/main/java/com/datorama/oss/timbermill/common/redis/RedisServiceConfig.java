package com.datorama.oss.timbermill.common.redis;

public class RedisServiceConfig {
    private final String redisHost;
    private final int redisPort;
    private final String redisPass;
    private final String redisMaxMemory;
    private final String redisMaxMemoryPolicy;
    private final boolean redisUseSsl;
    private final int redisTtlInSeconds;
    private final int redisGetSize;
    private final int redisPoolMinIdle;
    private final int redisPoolMaxIdle;
    private final int redisPoolMaxTotal;
    private int redisMaxTries;

    public RedisServiceConfig(String redisHost, int redisPort, String redisPass, String redisMaxMemory, String redisMaxMemoryPolicy, boolean redisUseSsl, int redisTtlInSeconds, int redisGetSize, int redisPoolMinIdle, int redisPoolMaxIdle, int redisPoolMaxTotal, int redisMaxTries) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisPass = redisPass;
        this.redisMaxMemory = redisMaxMemory;
        this.redisMaxMemoryPolicy = redisMaxMemoryPolicy;
        this.redisUseSsl = redisUseSsl;
        this.redisTtlInSeconds = redisTtlInSeconds;
        this.redisGetSize = redisGetSize;
        this.redisPoolMinIdle = redisPoolMinIdle;
        this.redisPoolMaxIdle = redisPoolMaxIdle;
        this.redisPoolMaxTotal = redisPoolMaxTotal;
        this.redisMaxTries = redisMaxTries;
    }

    public String getRedisHost() {
        return redisHost;
    }

    public int getRedisPort() {
        return redisPort;
    }

    public String getRedisPass() {
        return redisPass;
    }

    public String getRedisMaxMemory() {
        return redisMaxMemory;
    }

    public String getRedisMaxMemoryPolicy() {
        return redisMaxMemoryPolicy;
    }

    public boolean isRedisUseSsl() {
        return redisUseSsl;
    }

    public int getRedisTtlInSeconds() {
        return redisTtlInSeconds;
    }

    public int getRedisGetSize() {
        return redisGetSize;
    }

    public int getRedisPoolMinIdle() {
        return redisPoolMinIdle;
    }

    public int getRedisPoolMaxIdle() {
        return redisPoolMaxIdle;
    }

    public int getRedisPoolMaxTotal() {
        return redisPoolMaxTotal;
    }

    public int getRedisMaxTries() {
        return redisMaxTries;
    }

}

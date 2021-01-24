package com.datorama.oss.timbermill.common.cache;

public class CacheHandlerUtil {
    public static AbstractCacheHandler getCacheHandler(String strategy, long maximumTasksCacheWeight, long maximumOrphansCacheWeight, String redisHost, int redisPort, String redisPass, String redisMaxMemory) {
        if (strategy.compareToIgnoreCase("redis") == 0){
            return new RedisCacheHandler(maximumOrphansCacheWeight, redisHost, redisPort, redisPass, redisMaxMemory);
        }
        else {
            return new LocalCacheHandler(maximumTasksCacheWeight, maximumOrphansCacheWeight);
        }
    }
}
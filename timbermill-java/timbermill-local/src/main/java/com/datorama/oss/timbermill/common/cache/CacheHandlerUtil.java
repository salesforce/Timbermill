package com.datorama.oss.timbermill.common.cache;

public class CacheHandlerUtil {
    public static AbstractCacheHandler getCacheHandler(String strategy, CacheConfig cacheParams) {
        if (strategy.compareToIgnoreCase("redis") == 0){
            return new RedisCacheHandler(cacheParams.getRedisService(), cacheParams.getCacheRedisTtlInSeconds(), cacheParams.getOrphansCacheRedisTtlInSeconds(), cacheParams.getEventsCacheRedisTtlInSeconds());
        }
        else {
            return new LocalCacheHandler(cacheParams.getMaximumTasksCacheWeight(), cacheParams.getMaximumOrphansCacheWeight());
        }
    }
}

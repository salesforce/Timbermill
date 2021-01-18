package com.datorama.oss.timbermill.common.cache;

public class CacheHandlerUtil {
    public static CacheHandler getCacheHandler(String strategy,long maximumTasksCacheWeight, long maximumOrphansCacheWeight) {
        return new LocalCacheHandler(maximumTasksCacheWeight, maximumOrphansCacheWeight);
    }
}

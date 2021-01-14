package com.datorama.oss.timbermill.common.cache;

public class CacheHandlerUtil {
    public static CacheHandler getCacheHandler(String strategy, long maximumCacheWeight) {
        return new LocalCacheHandler(maximumCacheWeight);
    }
}

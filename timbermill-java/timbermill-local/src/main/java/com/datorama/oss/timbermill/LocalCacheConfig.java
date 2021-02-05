package com.datorama.oss.timbermill;

public class LocalCacheConfig {
    private final long maximumTasksCacheWeight;
    private final long maximumOrphansCacheWeight;

    public LocalCacheConfig(long maximumTasksCacheWeight, long maximumOrphansCacheWeight) {
        this.maximumTasksCacheWeight = maximumTasksCacheWeight;
        this.maximumOrphansCacheWeight = maximumOrphansCacheWeight;
    }

    public long getMaximumTasksCacheWeight() {
        return maximumTasksCacheWeight;
    }

    public long getMaximumOrphansCacheWeight() {
        return maximumOrphansCacheWeight;
    }
}

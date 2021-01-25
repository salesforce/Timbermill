package com.datorama.oss.timbermill.common.cache;

import com.datorama.oss.timbermill.common.KamonConstants;
import com.datorama.oss.timbermill.unit.LocalTask;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public abstract class AbstractCacheHandler {
    private Cache<String, List<String>> orphansCache;

    public AbstractCacheHandler(long maximumOrphansCacheWeight) {
        orphansCache = CacheBuilder.newBuilder()
                .maximumWeight(maximumOrphansCacheWeight)
                .weigher(this::getEntryLength)
                .removalListener(notification -> {
                    int entryLength = getEntryLength(notification.getKey(), notification.getValue());
                    KamonConstants.ORPHANS_CACHE_SIZE_RANGE_SAMPLER.withoutTags().decrement(entryLength);
                    KamonConstants.ORPHANS_CACHE_ENTRIES_RANGE_SAMPLER.withoutTags().decrement();
                })
                .build();
    }

    private int getEntryLength(String key, List<String> value) {
        int valuesLengths = value.stream().mapToInt(String::length).sum();
        int keyLength = key.length();
        return 2 * (keyLength + valuesLengths);
    }

    public List<String> pullFromOrphansCache(String parentId) {
        List<String> orphans = getFromOrphansCache(parentId);
        if (orphans != null){
            orphansCache.invalidate(parentId);
        }
        return orphans;
    }

    public List<String> getFromOrphansCache(String parentId) {
        return orphansCache.getIfPresent(parentId);
    }

    public void pushToOrphanCache(String parentId, List<String> tasks) {
        orphansCache.put(parentId, tasks);
        int entryLength = getEntryLength(parentId, tasks);
        KamonConstants.ORPHANS_CACHE_SIZE_RANGE_SAMPLER.withoutTags().increment(entryLength);
        KamonConstants.ORPHANS_CACHE_ENTRIES_RANGE_SAMPLER.withoutTags().increment();
    }

    public abstract Map<String, LocalTask> getFromTasksCache(Collection<String> idsList);

    public abstract void pushToTasksCache(Map<String, LocalTask> idsToMap);
}

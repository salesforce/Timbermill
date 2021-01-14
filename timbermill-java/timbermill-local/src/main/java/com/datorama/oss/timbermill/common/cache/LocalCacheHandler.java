package com.datorama.oss.timbermill.common.cache;

import com.datorama.oss.timbermill.common.KamonConstants;
import com.datorama.oss.timbermill.unit.LocalTask;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;

import java.util.List;

public class LocalCacheHandler implements CacheHandler {
    private Cache<String, LocalTask> tasksCache;
    private Cache<String, List<String>> orphansCache;

    LocalCacheHandler(long maximumCacheWeight) {
        tasksCache = CacheBuilder.newBuilder()
                .maximumWeight(maximumCacheWeight)
                .weigher((Weigher<String, LocalTask>) (key, value) -> key.length() + value.estimatedSize())
                .removalListener(notification -> {
                    String key = notification.getKey();
                    LocalTask value = notification.getValue();
                    KamonConstants.TASK_CACHE_SIZE_RANGE_SAMPLER.withoutTags().decrement(key.length() + value.estimatedSize());
                })
                .build();
        orphansCache = CacheBuilder.newBuilder()
                .maximumWeight(maximumCacheWeight)
                .weigher((Weigher<String, List<String>>) (key, value) -> key.length() + value.stream().mapToInt(String::length).sum())
                .removalListener(notification -> {
                    int entryLength = getEntryLength(notification.getKey(), notification.getValue());
                    KamonConstants.ORPHANS_CACHE_SIZE_RANGE_SAMPLER.withoutTags().decrement(entryLength);
                })
                .build();
    }

    private int getEntryLength(String key, List<String> value) {
        int valuesLengths = value.stream().mapToInt(String::length).sum();
        int keyLength = key.length();
        return keyLength + valuesLengths;
    }

    @Override
    public LocalTask getFromTasksCache(String id) {
        return tasksCache.getIfPresent(id);
    }

    @Override
    public long orphansCacheSize() {
        return orphansCache.size();
    }

    @Override
    public List<String> pullFromOrphansCache(String parentId) {
        List<String> orphans = orphansCache.getIfPresent(parentId);
        if (orphans != null){
            orphansCache.invalidate(parentId);
        }
        return orphans;
    }

    @Override
    public List<String> getFromOrphansCache(String parentId) {
        return orphansCache.getIfPresent(parentId);
    }

    @Override
    public void pushToOrphanCache(String parentId, List<String> tasks) {
        orphansCache.put(parentId, tasks);
        int entryLength = getEntryLength(parentId, tasks);
        KamonConstants.ORPHANS_CACHE_SIZE_RANGE_SAMPLER.withoutTags().increment(entryLength);
    }

    @Override
    public void pushToTasksCache(String id, LocalTask localTask) {
        tasksCache.put(id, localTask);
    }
}

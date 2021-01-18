package com.datorama.oss.timbermill.common.cache;

import com.datorama.oss.timbermill.common.KamonConstants;
import com.datorama.oss.timbermill.unit.LocalTask;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;

import java.util.List;

import static com.datorama.oss.timbermill.ElasticsearchClient.GSON;

public class LocalCacheHandler implements CacheHandler {
    private Cache<String, String> tasksCache;
    private Cache<String, List<String>> orphansCache;

    LocalCacheHandler(long maximumTasksCacheWeight, long maximumOrphansCacheWeight) {
        tasksCache = CacheBuilder.newBuilder()
                .maximumWeight(maximumTasksCacheWeight)
                .weigher((Weigher<String, String>) (key, value) -> 2 * (key.length() + value.length()))
                .removalListener(notification -> {
                    String key = notification.getKey();
                    String value = notification.getValue();
                    KamonConstants.TASK_CACHE_SIZE_RANGE_SAMPLER.withoutTags().decrement(2 * (key.length() + value.length()));
                    KamonConstants.TASK_CACHE_ENTRIES_RANGE_SAMPLER.withoutTags().decrement();
                })
                .build();
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

    @Override
    public LocalTask getFromTasksCache(String id) {
        String taskString = tasksCache.getIfPresent(id);
        return GSON.fromJson(taskString, LocalTask.class);
    }

    @Override
    public List<String> pullFromOrphansCache(String parentId) {
        List<String> orphans = getFromOrphansCache(parentId);
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
        KamonConstants.ORPHANS_CACHE_ENTRIES_RANGE_SAMPLER.withoutTags().increment();
    }

    @Override
    public void pushToTasksCache(String id, LocalTask localTask) {
        String taskString = GSON.toJson(localTask);
        tasksCache.put(id, taskString);
        KamonConstants.TASK_CACHE_SIZE_RANGE_SAMPLER.withoutTags().increment(2 * (id.length() + taskString.length()));
        KamonConstants.TASK_CACHE_ENTRIES_RANGE_SAMPLER.withoutTags().increment();
    }
}

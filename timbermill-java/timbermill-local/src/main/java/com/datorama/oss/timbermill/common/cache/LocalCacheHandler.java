package com.datorama.oss.timbermill.common.cache;

import com.datorama.oss.timbermill.common.KamonConstants;
import com.datorama.oss.timbermill.unit.LocalTask;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.datorama.oss.timbermill.ElasticsearchClient.GSON;

public class LocalCacheHandler extends AbstractCacheHandler {
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
    public Map<String, List<String>> pullFromOrphansCache(Set<String> parentsIds) {
        Map<String, List<String>> orphans = orphansCache.getAllPresent(parentsIds);
        if (orphans != null){
            orphansCache.invalidateAll(parentsIds);
        }
        return orphans;
    }

    @Override
    public void pushToOrphanCache(Map<String, List<String>> orphansMap) {
        orphansCache.putAll(orphansMap);
        int entryLength = orphansMap.entrySet().stream().mapToInt(value -> getEntryLength(value.getKey(), value.getValue())).sum();
        KamonConstants.ORPHANS_CACHE_SIZE_RANGE_SAMPLER.withoutTags().increment(entryLength);
        KamonConstants.ORPHANS_CACHE_ENTRIES_RANGE_SAMPLER.withoutTags().increment(orphansMap.size());
    }

    @Override
    public Map<String, LocalTask> getFromTasksCache(Collection<String> idsList) {
        Map<String, LocalTask> retMap = Maps.newHashMap();
        for (String id : idsList) {
            String taskString = tasksCache.getIfPresent(id);
            LocalTask localTask = GSON.fromJson(taskString, LocalTask.class);
            retMap.put(id, localTask);
        }
        return retMap;

    }

    @Override
    public void pushToTasksCache(Map<String, LocalTask> idsToMap) {
        for (Map.Entry<String, LocalTask> entry : idsToMap.entrySet()) {
            String id = entry.getKey();
            LocalTask localTask = entry.getValue();
            String taskString = GSON.toJson(localTask);
            tasksCache.put(id, taskString);
            KamonConstants.TASK_CACHE_SIZE_RANGE_SAMPLER.withoutTags().increment(2 * (id.length() + taskString.length()));
            KamonConstants.TASK_CACHE_ENTRIES_RANGE_SAMPLER.withoutTags().increment();
        }
    }

    @Override
    public void close() {
        tasksCache.cleanUp();
    }
}

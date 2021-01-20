package com.datorama.oss.timbermill.common.cache;

import com.datorama.oss.timbermill.common.KamonConstants;
import com.datorama.oss.timbermill.unit.LocalTask;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;

import static com.datorama.oss.timbermill.ElasticsearchClient.GSON;

public class LocalCacheHandler extends AbstractCacheHandler {
    private Cache<String, String> tasksCache;

    LocalCacheHandler(long maximumTasksCacheWeight, long maximumOrphansCacheWeight) {
        super(maximumOrphansCacheWeight);
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

    }

    @Override
    public LocalTask getFromTasksCache(String id) {
        String taskString = tasksCache.getIfPresent(id);
        return GSON.fromJson(taskString, LocalTask.class);
    }

    @Override
    public void pushToTasksCache(String id, LocalTask localTask) {
        String taskString = GSON.toJson(localTask);
        tasksCache.put(id, taskString);
        KamonConstants.TASK_CACHE_SIZE_RANGE_SAMPLER.withoutTags().increment(2 * (id.length() + taskString.length()));
        KamonConstants.TASK_CACHE_ENTRIES_RANGE_SAMPLER.withoutTags().increment();
    }
}

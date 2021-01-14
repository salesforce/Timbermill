package com.datorama.oss.timbermill.common.cache;

import com.datorama.oss.timbermill.unit.LocalTask;

import java.util.List;

public interface CacheHandler {
    LocalTask getFromTasksCache(String id);

    long orphansCacheSize();

    List<String> pullFromOrphansCache(String parentId);

    List<String> getFromOrphansCache(String parentId);

    void pushToOrphanCache(String parentId, List<String> tasks);

    void pushToTasksCache(String id, LocalTask localTask);
}

package com.datorama.oss.timbermill.common.cache;

import com.datorama.oss.timbermill.common.KamonConstants;
import com.datorama.oss.timbermill.unit.LocalTask;
import kamon.metric.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class AbstractCacheHandler {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractCacheHandler.class);

    public Map<String, LocalTask> logGetFromTasksCache(Collection<String> idsList, String type){
        Timer.Started start = KamonConstants.RETRIEVE_FROM_TASKS_CACHE_TIMER.withTag("type", type).start();
        Map<String, LocalTask> retMap = getFromTasksCache(idsList);
        start.stop();
        KamonConstants.TASKS_QUERIED_FROM_CACHE_HISTOGRAM.withTag("type", type).record(idsList.size());
        KamonConstants.TASKS_RETRIEVED_FROM_CACHE_HISTOGRAM.withTag("type", type).record(retMap.size());
        LOG.debug("{} tasks retrieved from cache, flow: [{}]", retMap.size(), type);
        return retMap;
    }

    public void logPushToTasksCache(Map<String, LocalTask> idsToMap, String type){
        LOG.debug("Pushing {} tasks to cache, flow: [{}]", idsToMap.size(), type);
        Timer.Started start = KamonConstants.PUSH_TO_CACHE_TIMER.withTag("type", type).start();
        pushToTasksCache(idsToMap);
        start.stop();
        KamonConstants.TASKS_PUSHED_TO_CACHE_HISTOGRAM.withTag("type", type).record(idsToMap.size());
    }

    public Map<String, List<String>> logPullFromOrphansCache(Set<String> parentsIds, String type){
        LOG.debug("Pulling {} parents from orphan cache, flow: [{}]", parentsIds.size(), type);
        Timer.Started start = KamonConstants.PULL_FROM_ORPHAN_CACHE_TIMER.withTag("type", type).start();
        Map<String, List<String>> retMap = pullFromOrphansCache(parentsIds);
        start.stop();
        KamonConstants.PARENTS_RETRIEVED_FROM_ORPHAN_CACHE_HISTOGRAM.withTag("type", type).record(retMap.size());
        LOG.debug("{} parents retrieved from orphan cache, flow: [{}]", retMap.size(), type);
        return retMap;
    }

    public void logPushToOrphanCache(Map<String, List<String>> orphansMap, String type){
        LOG.debug("Pushing {} parents to orphan cache, flow: [{}]", orphansMap.size(), type);
        Timer.Started start = KamonConstants.PUSH_TO_ORPHAN_CACHE_TIMER.withTag("type", type).start();
        pushToOrphanCache(orphansMap);
        start.stop();
        KamonConstants.PARENTS_PUSHED_TO_ORPHAN_CACHE_HISTOGRAM.withTag("type", type).record(orphansMap.size());
    }

    abstract Map<String, List<String>> pullFromOrphansCache(Collection<String> parentsIds);

    abstract void pushToOrphanCache(Map<String, List<String>> orphansMap);

    abstract Map<String, LocalTask> getFromTasksCache(Collection<String> idsList);

    abstract void pushToTasksCache(Map<String, LocalTask> idsToMap);

    public abstract void close();

    public void lock() {
        //do nothing
    }

    public void release() {
        //do nothing
    }
}

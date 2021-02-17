package com.datorama.oss.timbermill;

import com.datorama.oss.timbermill.common.cache.AbstractCacheHandler;
import com.datorama.oss.timbermill.unit.Event;
import com.datorama.oss.timbermill.unit.LocalTask;
import com.datorama.oss.timbermill.unit.Task;
import com.datorama.oss.timbermill.unit.TaskStatus;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

class ParentResolver {
    private static final Logger LOG = LoggerFactory.getLogger(ParentResolver.class);

    private Map<String, Task> receivedTasksMap;
    private AbstractCacheHandler cacheHandler;

    ParentResolver(Map<String, Task> receivedTasksMap, AbstractCacheHandler cacheHandler) {
        this.receivedTasksMap = receivedTasksMap;
        this.cacheHandler = cacheHandler;
    }

    Map<String, Task> resolveOrphansReceived() {
        return resolveOrphans(receivedTasksMap);
    }

    private Map<String, Task> resolveOrphans(Map<String, Task> potentialAdoptingTasks) {
        Set<String> adoptingCandidates = potentialAdoptingTasks.entrySet().stream().filter(entry -> {
            Task parentIndexedTask = entry.getValue();
            boolean isParentStartedTask = parentIndexedTask.getStatus() == TaskStatus.UNTERMINATED || parentIndexedTask.getStatus() == TaskStatus.SUCCESS || parentIndexedTask.getStatus() == TaskStatus.ERROR;
            boolean isParentNotOrphan = parentIndexedTask.isOrphan() == null || !parentIndexedTask.isOrphan();
            return isParentStartedTask && isParentNotOrphan;
        }).map(Map.Entry::getKey).collect(Collectors.toSet());

        Map<String, LocalTask> orphansMap = findAdoptedOrphansInCache(adoptingCandidates);
        return getEnrichedAdoptedOrphans(potentialAdoptingTasks, orphansMap);
    }

    private Map<String, Task> resolveOrphansWithAdoptedOrphans(Map<String, Task> potentialAdoptingTasks) {
        Set<String> adoptingCandidates = potentialAdoptingTasks.keySet();
        Map<String, Task> orphansMap = findAdoptedOrphans(adoptingCandidates);
        return getEnrichedAdoptedOrphans(potentialAdoptingTasks, orphansMap);
    }

    private Map<String, Task> getEnrichedAdoptedOrphans(Map<String, Task> potentialAdoptingTasks, Map<String, ? extends Task> orphansMap) {
        Map<String, Task> adoptedTasksMap = Maps.newHashMap();
        for (Map.Entry<String, ? extends Task> entry : orphansMap.entrySet()) {
            String adoptedId = entry.getKey();
            Task adoptedTask = entry.getValue();
            if (adoptedTask == null) {
                LOG.warn("Missing adopted task from cache {}", adoptedId);
            } else {
                adoptedTask.setOrphan(false);
                populateParentParams(adoptedTask, potentialAdoptingTasks.get(adoptedTask.getParentId()));
                adoptedTasksMap.put(adoptedId, adoptedTask);
            }
        }
        if (!adoptedTasksMap.isEmpty()) {
            LOG.debug("Resolving orphans using {} adopted orphans", adoptedTasksMap.size());
            Map<String, Task> adoptedTasksNewlyAdoptedTask = resolveOrphansWithAdoptedOrphans(adoptedTasksMap);
            adoptedTasksMap.putAll(adoptedTasksNewlyAdoptedTask);
        }
        return adoptedTasksMap;
    }

    private Map<String, Task> findAdoptedOrphans(Set<String> adoptingCandidates) {
        Map<String, LocalTask> adoptedOrphansTasksFromCache = findAdoptedOrphansInCache(adoptingCandidates);
        Map<String, Task> adoptedOrphansReceivedTasks = findAdoptedOrphansInReceivedTasks(adoptingCandidates);

        Map<String, Task> orphansMap = Maps.newHashMap();
        orphansMap.putAll(adoptedOrphansTasksFromCache);
        orphansMap.putAll(adoptedOrphansReceivedTasks);

        return orphansMap;
    }

    private Map<String, Task> findAdoptedOrphansInReceivedTasks(Set<String> adoptingCandidates) {
        Map<String, Task> retMap = Maps.newHashMap();
        for (Map.Entry<String, Task> entry : receivedTasksMap.entrySet()) {
            Task task = entry.getValue();
            if (task.isOrphan() != null && task.isOrphan() && adoptingCandidates.contains(task.getParentId())){
                retMap.put(entry.getKey(), task);
            }
        }
        return retMap;
    }

    private Map<String, LocalTask> findAdoptedOrphansInCache(Set<String> adoptingCandidates) {
        Map<String, List<String>> adoptedOrphansFromCache = cacheHandler.logPullFromOrphansCache(adoptingCandidates, "resolve_orphans");
        if (adoptedOrphansFromCache.isEmpty()){
            return Maps.newHashMap();
        }
        else {
            List<String> orphansIds = adoptedOrphansFromCache.values().stream().flatMap(List::stream).collect(Collectors.toList());
            return cacheHandler.logGetFromTasksCache(orphansIds, "resolve_orphans");
        }
    }

    private static void populateParentParams(Task task, Task parentIndexedTask) {
        ParentProperties parentProperties = getParentProperties(parentIndexedTask, null);

        List<String> parentsPath = getParentPath(parentProperties);
        if(!parentsPath.isEmpty()) {
            task.setParentsPath(parentsPath);
        }

        task.setPrimaryId(parentProperties.getPrimaryId());
        if (task.getCtx() == null){
            task.setCtx(Maps.newHashMap());
        }
        for (Map.Entry<String, String> entry : parentProperties.getContext().entrySet()) {
            task.getCtx().putIfAbsent(entry.getKey(), entry.getValue());
        }
    }

    static void populateParentParams(Event event, Task parentIndexedTask, Collection<Event> parentCurrentEvent) {
        ParentProperties parentProperties = getParentProperties(parentIndexedTask, parentCurrentEvent);

        List<String> parentsPath = getParentPath(parentProperties);
        if(!parentsPath.isEmpty()) {
            event.setParentsPath(parentsPath);
        }

        event.setPrimaryId(parentProperties.getPrimaryId());
        if (event.getContext() == null){
            event.setContext(Maps.newHashMap());
        }
        for (Map.Entry<String, String> entry : parentProperties.getContext().entrySet()) {
            event.getContext().putIfAbsent(entry.getKey(), entry.getValue());
        }
    }

    private static List<String> getParentPath(ParentProperties parentProperties) {
        List<String> parentsPath = new ArrayList<>();
        Collection<String> parentParentsPath = parentProperties.getParentPath();
        if ((parentParentsPath != null) && !parentParentsPath.isEmpty()) {
            parentsPath.addAll(parentParentsPath);
        }
        String parentName = parentProperties.getParentName();
        if (parentName != null) {
            parentsPath.add(parentName);
        }
        return parentsPath;
    }

    private static ParentProperties getParentProperties(Task parentIndexedTask, Collection<Event> parentCurrentEvent) {
        Map<String, String> context = Maps.newHashMap();
        String primaryId = null;
        Collection<String> parentPath = null;
        String parentName = null;
        if (parentCurrentEvent != null && !parentCurrentEvent.isEmpty()){
            for (Event previousEvent : parentCurrentEvent) {
                String previousPrimaryId = previousEvent.getPrimaryId();
                if (previousPrimaryId != null){
                    primaryId = previousPrimaryId;
                }
                Map<String, String> previousContext = previousEvent.getContext();
                if (previousContext != null){
                    context.putAll(previousContext);
                }
                Collection<String> previousPath = previousEvent.getParentsPath();
                if (previousPath != null){
                    parentPath = previousPath;
                }

                String previousName = previousEvent.getName();
                if (previousName != null){
                    parentName = previousName;
                }
            }
        }
        if (parentIndexedTask != null){

            String indexedPrimary = parentIndexedTask.getPrimaryId();
            if (indexedPrimary != null) {
                primaryId = indexedPrimary;
            }

            Map<String, String> indexedContext = parentIndexedTask.getCtx();
            if (indexedContext != null) {
                context.putAll(indexedContext);
            }

            List<String> indexedParentsPath = parentIndexedTask.getParentsPath();
            if (indexedParentsPath != null) {
                parentPath = indexedParentsPath;
            }

            String indexedName = parentIndexedTask.getName();
            if (indexedName != null) {
                parentName = indexedName;
            }
        }

        return new ParentProperties(primaryId, context, parentPath, parentName);
    }

    static class ParentProperties{

        private final String primaryId;
        private final Map<String, String> context;
        private final Collection<String> parentPath;
        private final String parentName;

        ParentProperties(String primaryId, Map<String, String> context, Collection<String> parentPath, String parentName) {
            this.primaryId = primaryId;
            this.context = context;
            this.parentPath = parentPath;
            this.parentName = parentName;
        }

        String getPrimaryId() {
            return primaryId;
        }

        Map<String, String> getContext() {
            return context;
        }

        Collection<String> getParentPath() {
            return parentPath;
        }

        String getParentName() {
            return parentName;
        }

    }
}

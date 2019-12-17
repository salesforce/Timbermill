package com.datorama.timbermill;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.swing.tree.DefaultMutableTreeNode;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.timbermill.common.Constants;
import com.datorama.timbermill.plugins.PluginsConfig;
import com.datorama.timbermill.plugins.TaskLogPlugin;
import com.datorama.timbermill.unit.*;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import static com.datorama.timbermill.common.Constants.GSON;
import static com.datorama.timbermill.unit.Task.TaskStatus;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toSet;

public class TaskIndexer {

    private static final Logger LOG = LoggerFactory.getLogger(TaskIndexer.class);
    private static final String TEXT = "text";
    private static final String STRING = "string";
    private static final String CTX = "ctx";

    private final ElasticsearchClient es;
    private final Collection<TaskLogPlugin> logPlugins;
    private final Map<String, Integer> propertiesLengthMap;
    private final int defaultMaxChars;
    private final Cache<String, Queue<Event>> parentIdTORootOrphansEventsCache;

    public TaskIndexer(ElasticsearchParams elasticsearchParams) {
        logPlugins = PluginsConfig.initPluginsFromJson(elasticsearchParams.getPluginsJson());
        this.propertiesLengthMap = elasticsearchParams.getPropertiesLengthJson();
        this.defaultMaxChars = elasticsearchParams.getDefaultMaxChars();
        this.es = new ElasticsearchClient(elasticsearchParams.getElasticUrl(), elasticsearchParams.getIndexBulkSize(), elasticsearchParams.getDaysRotation(), elasticsearchParams.getIndexingThreads(),
                elasticsearchParams.getAwsRegion(), elasticsearchParams.getElasticUser(), elasticsearchParams.getElasticPassword());
        parentIdTORootOrphansEventsCache = CacheBuilder.newBuilder().maximumSize(elasticsearchParams.getMaximumCacheSize()).expireAfterAccess(elasticsearchParams.getMaximumCacheMinutesHold(), TimeUnit.MINUTES).build();
    }

    public void retrieveAndIndex(Collection<Event> events, String env) {
        LOG.info("------------------ Batch Start ------------------");
        ZonedDateTime taskIndexerStartTime = ZonedDateTime.now();
        trimAllStrings(events);

        Collection<Event> heartbeatEvents = new HashSet<>();
        Collection<Event> timbermillEvents = new LinkedHashSet<>();
        Collection<String> startEventsIds = new HashSet<>();

        events.forEach(e -> {
            if (e.getName() != null && e.getName().equals(Constants.HEARTBEAT_TASK)){
                heartbeatEvents.add(e);
            }
            else{
                timbermillEvents.add(e);
                if (e.isStartEvent()){
                    startEventsIds.add(e.getTaskId());
                }
            }
        });

        if (!heartbeatEvents.isEmpty()) {
            indexHeartbeatEvents(env, heartbeatEvents);
        }

        LOG.info("{} events to handle in current batch", timbermillEvents.size());

        if (!timbermillEvents.isEmpty()) {
            timbermillEvents.forEach(e -> LOG.error("{} -> {}", e.getTaskId(), e.getClass()));
            IndexEvent indexEvent = handleTimbermillEvents(env, taskIndexerStartTime, timbermillEvents, startEventsIds);
            es.indexMetaDataEvents(env, GSON.toJson(indexEvent));
        }
        LOG.info("------------------ Batch End ------------------");
    }

    private IndexEvent handleTimbermillEvents(String env, ZonedDateTime taskIndexerStartTime, Collection<Event> timbermillEvents, Collection<String> startEventsIds) {
        Map<String, Task> previouslyIndexedParentTasks = new HashMap<>();
        long pluginsDuration = 0L;
        try {
            previouslyIndexedParentTasks = fetchPreviouslyIndexedParentTasks(timbermillEvents, startEventsIds);
            pluginsDuration = applyPlugins(timbermillEvents, env);

            Map<String, Task> tasksMap = createEnrichedTasks(env, timbermillEvents, previouslyIndexedParentTasks);

            es.index(tasksMap);
            LOG.info("{} tasks were indexed to elasticsearch", timbermillEvents.size());

            return new IndexEvent(timbermillEvents.size(), previouslyIndexedParentTasks.size(), taskIndexerStartTime, ZonedDateTime.now(), pluginsDuration);
        } catch (Throwable t) {
            LOG.error("Error while handling Timbermill events", t);
            return new IndexEvent(timbermillEvents.size(),  previouslyIndexedParentTasks.size(), taskIndexerStartTime, ZonedDateTime.now(), ExceptionUtils.getStackTrace(t), pluginsDuration);
        }
    }

    public static long getTimesDuration(ZonedDateTime taskIndexerStartTime, ZonedDateTime taskIndexerEndTime) {
        return ChronoUnit.MILLIS.between(taskIndexerStartTime, taskIndexerEndTime);
    }

    private Map<String, Task> fetchPreviouslyIndexedParentTasks(Collection<Event> timbermillEvents, Collection<String> startEventsIds) {
        Set<String> missingParentTaskIds = getMissingParentTaskIds(timbermillEvents, startEventsIds);
        Map<String, Task> previouslyIndexedParentTasks = Maps.newHashMap();
        if (!missingParentTaskIds.isEmpty()) {
            previouslyIndexedParentTasks = this.es.fetchIndexedTasks(missingParentTaskIds);
        }
        return previouslyIndexedParentTasks;
    }

    private Map<String, Task> createEnrichedTasks(String env, Collection<Event> timbermillEvents, Map<String, Task> previouslyIndexedParentTasks) {
        Map<String, List<Event>> eventsMap = timbermillEvents.stream().collect(groupingBy(Event::getTaskId));
        Collection<DefaultMutableTreeNode> roots = getTreesRoots(timbermillEvents);
        enrichEvents(roots, eventsMap, previouslyIndexedParentTasks);
        return getTasksFromEvents(env, eventsMap);
    }

    private Map<String, Task> getTasksFromEvents(String env, Map<String, List<Event>> eventsMap) {
        Map<String, Task> tasksMap = new HashMap<>();
        for (Map.Entry<String, List<Event>> eventEntry : eventsMap.entrySet()) {
            Task task = new Task(eventEntry.getValue(), env);
            tasksMap.put(eventEntry.getKey(), task);
        }
        return tasksMap;
    }

    private void indexHeartbeatEvents(String env, Collection<Event> heartbeatEvents) {
        String[] metadataEvents = heartbeatEvents.stream().map(event -> GSON.toJson(new HeartbeatTask(event))).toArray(String[]::new);
        this.es.indexMetaDataEvents(env, metadataEvents);
    }

    private void enrichEvents(Collection<DefaultMutableTreeNode> roots, Map<String, List<Event>> eventsMap, Map<String, Task> previouslyIndexedTasks) {
        /*
         * Compute origins and down merge parameters from parent
         */
        for (DefaultMutableTreeNode root : roots) {
            Enumeration enumeration = root.breadthFirstEnumeration();
            while (enumeration.hasMoreElements()){
                DefaultMutableTreeNode curr = (DefaultMutableTreeNode) enumeration.nextElement();
                Event event = (Event) curr.getUserObject();
                enrichEvent(eventsMap, previouslyIndexedTasks, event);
            }
        }
    }

    private void updateAdoptedOrphans(Event event, Map<String, List<Event>> eventsMap, Map<String, Task> previouslyIndexedTasks) {
        String taskId = event.getTaskId();
        Queue<Event> orphansEvents = parentIdTORootOrphansEventsCache.getIfPresent(taskId);
        if (orphansEvents != null && (event.isOrphan() == null || !event.isOrphan())){
            parentIdTORootOrphansEventsCache.invalidate(taskId);
            for (Event orphanEvent : orphansEvents) {
                if (!eventsMap.containsKey(event.getTaskId())){
					eventsMap.put(event.getTaskId(), Lists.newArrayList(event));
				}
                String orphanId = orphanEvent.getTaskId();
                if (eventsMap.containsKey(orphanId)){
                    eventsMap.get(orphanId).add(orphanEvent);
                }
                else{
                    List<Event> eventsList = new LinkedList<>();
                    eventsList.add(orphanEvent);
                    eventsMap.put(orphanId, eventsList);
                }
                enrichEvent(eventsMap, previouslyIndexedTasks, orphanEvent);

            }
        }
    }

    private void enrichEvent(Map<String, List<Event>> eventsMap, Map<String, Task> previouslyIndexedTasks, Event event) {
        List<String> parentsPath = new ArrayList<>();
        String parentId = event.getParentId();
        if (parentId != null) {

            if (isOrphan(previouslyIndexedTasks, event.getParentId(), event, eventsMap)){
                event.setOrphan(true);
                Queue<Event> eventList = parentIdTORootOrphansEventsCache.getIfPresent(event.getParentId());

                Event orphanEvent = new AdoptedEvent(event);
                if (eventList == null) {
                    eventList = new LinkedBlockingQueue<>();
                    eventList.add(orphanEvent);
                    parentIdTORootOrphansEventsCache.put(event.getParentId(), eventList);
                } else {
                    eventList.add(orphanEvent);
                }
            }
            else {
                event.setOrphan(false);
            }

            ParentProperties parentProperties = getParentProperties(previouslyIndexedTasks.get(parentId), eventsMap.get(parentId));

            String primaryId = parentProperties.getPrimaryId();
            event.setPrimaryId(primaryId);
            for (Map.Entry<String, String> entry : parentProperties.getContext().entrySet()) {
                event.getContext().putIfAbsent(entry.getKey(), entry.getValue());
            }

            Collection<String> parentParentsPath = parentProperties.getParentPath();
            if((parentParentsPath != null) && !parentParentsPath.isEmpty()) {
                parentsPath.addAll(parentParentsPath);
            }

            String parentName = parentProperties.getParentName();
            if(parentName != null) {
                parentsPath.add(parentName);
            }
        }
        if(!parentsPath.isEmpty()) {
            event.setParentsPath(parentsPath);
        }
        updateAdoptedOrphans(event, eventsMap, previouslyIndexedTasks);
    }

    private boolean isOrphan(Map<String, Task> previouslyIndexedTasks, String parentId, Event event, Map<String, List<Event>> eventsMap) {
        if (event.isOrphan() != null && !event.isOrphan()){
            return false;
        }
        if (parentId != null) {
            if (parentIdTORootOrphansEventsCache.getIfPresent(parentId) != null){
                return true;
            }
            else if(eventsMap.containsKey(parentId)){
                if (eventsMap.get(parentId).stream().anyMatch(e -> e instanceof AdoptedEvent)){
                    return false;
                }
            }

            if (previouslyIndexedTasks.containsKey(parentId)) {
                Boolean isOrphan = previouslyIndexedTasks.get(parentId).isOrphan();
                return isOrphan != null && isOrphan;
            }
            else {
                return true;
            }
        }
        else {
            return false;
        }

    }

    private Collection<DefaultMutableTreeNode> getTreesRoots(Collection<Event> timbermillEvents) {
        Map<String, DefaultMutableTreeNode> nodesMap = new HashMap<>();
        for (Event timbermillEvent : timbermillEvents) {
            if (timbermillEvent.isStartEvent()) {
                nodesMap.put(timbermillEvent.getTaskId(), new DefaultMutableTreeNode(timbermillEvent));
            }
        }
        for (DefaultMutableTreeNode treeNode : nodesMap.values()) {

            Event startEvent = (Event) treeNode.getUserObject();
            String parentId = startEvent.getParentId();
            if (parentId != null) {
                DefaultMutableTreeNode parentNode = nodesMap.get(parentId);
                if (parentNode != null) {
                    parentNode.add(treeNode);
                }
            }
        }

        Collection<DefaultMutableTreeNode> roots = new HashSet<>();
        for (DefaultMutableTreeNode node : nodesMap.values()) {
            roots.add((DefaultMutableTreeNode) node.getRoot());
        }
        return roots;
    }


    private long applyPlugins(Collection<Event> events, String env) {
        long pluginsStart = System.currentTimeMillis();
        try {
            for (TaskLogPlugin plugin : logPlugins) {
                ZonedDateTime startTime = ZonedDateTime.now();
                TaskStatus status;
                String exception = null;
                try {
                    plugin.apply(events);
                    status = TaskStatus.SUCCESS;
                } catch (Exception ex) {
                    exception = ExceptionUtils.getStackTrace(ex);
                    status = TaskStatus.ERROR;
                    LOG.error("error in plugin" + plugin, ex);
                }
                ZonedDateTime endTime = ZonedDateTime.now();
                long duration = getTimesDuration(startTime, endTime);
                PluginApplierEvent pluginApplierEvent = new PluginApplierEvent(startTime, plugin.getName(), plugin.getClass().getSimpleName(), status, exception, endTime, duration);
                es.indexMetaDataEvents(env, GSON.toJson(pluginApplierEvent));
            }
        } catch (Exception ex) {
            LOG.error("Error running plugins", ex);
        }
        long pluginsEnd = System.currentTimeMillis();
        return pluginsEnd - pluginsStart;
    }

    private void trimAllStrings(Collection<Event> events) {
        events.forEach(e -> {
            e.setStrings(getTrimmedLongValues(e.getStrings(), STRING));
            e.setTexts(getTrimmedLongValues(e.getTexts(), TEXT));
            e.setContext(getTrimmedLongValues(e.getContext(), CTX));
        });
    }

    private Map<String, String> getTrimmedLongValues(Map<String, String> oldMap, String prefix) {
        Map<String, String> newMap = new HashMap<>();
        for (Map.Entry<String, String> entry : oldMap.entrySet()){
            String key = entry.getKey();
            String value = trimIfNeededValue(prefix + "." + key, entry.getValue());
            newMap.put(key, value);
        }
        return newMap;
    }

    private String trimIfNeededValue(String key, String value) {
        Integer maxPropertyLength = propertiesLengthMap.get(key);
        if (maxPropertyLength != null){
            if (value.length() > maxPropertyLength) {
                return value.substring(0, maxPropertyLength);
            }
        }
        else if (value.length() > defaultMaxChars){
            return value.substring(0, defaultMaxChars);
        }
        return value;
    }

    private static Set<String> getMissingParentTaskIds(Collection<Event> timbermillEvents, Collection<String> startEventsIds) {

        return timbermillEvents.stream()
                .filter(e -> e.getParentId() != null && !startEventsIds.contains(e.getParentId()))
                .map(Event::getParentId)
                .collect(toSet());
    }

    private static ParentProperties getParentProperties(Task indexedTask, Collection<Event> previousEvents) {

        Map<String, String> context = Maps.newHashMap();
        String primaryId = null;
        Collection<String> parentPath = null;
        String parentName = null;
        if (indexedTask != null){
            primaryId = indexedTask.getPrimaryId();
            context = indexedTask.getCtx();
            parentPath = indexedTask.getParentsPath();
            parentName = indexedTask.getName();
        }

        if (previousEvents != null){
            for (Event previousEvent : previousEvents) {
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
        return new ParentProperties(primaryId, context, parentPath, parentName);
    }

    public void close() {
        es.close();
    }

    private static class ParentProperties{

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

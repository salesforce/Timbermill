package com.datorama.timbermill;


import com.datorama.timbermill.common.Constants;
import com.datorama.timbermill.plugins.PluginsConfig;
import com.datorama.timbermill.plugins.TaskLogPlugin;
import com.datorama.timbermill.unit.*;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.elasticsearch.ElasticsearchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.tree.DefaultMutableTreeNode;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.datorama.timbermill.common.Constants.GSON;
import static com.datorama.timbermill.unit.Task.TaskStatus;
import static java.util.stream.Collectors.*;

public class TaskIndexer {

    private static final Logger LOG = LoggerFactory.getLogger(TaskIndexer.class);
    private static final String TEXT = "text";
    private static final String STRING = "string";
    private static final String CTX = "ctx";

    private final ElasticsearchClient es;
    private final Collection<TaskLogPlugin> logPlugins;
    private final Map<String, Integer> propertiesLengthMap;
    private final int defaultMaxChars;
    private final Cache<String, Event> cache;

    public TaskIndexer(String pluginsJson, Map<String, Integer> propertiesLengthJson, int defaultMaxChars, String elasticUrl, int daysRotation, String awsRegion, int indexBulkSize, int indexingThreads, String elasticUser, String elasticPassword) {
        logPlugins = PluginsConfig.initPluginsFromJson(pluginsJson);
        this.propertiesLengthMap = propertiesLengthJson;
        this.defaultMaxChars = defaultMaxChars;
        this.es = new ElasticsearchClient(elasticUrl, indexBulkSize, daysRotation, awsRegion, indexingThreads, elasticUser, elasticPassword);
        cache = CacheBuilder.newBuilder().maximumSize(10000).expireAfterAccess(6, TimeUnit.HOURS).build();
    }

    public void retrieveAndIndex(List<Event> events, String env) {
        LOG.info("------------------ Batch Start ------------------");
        ZonedDateTime taskIndexerStartTime = ZonedDateTime.now();
        trimAllStrings(events);

        List<Event> heartbeatEvents = events.stream().filter(e -> (e.getName() != null) && e.getName().equals(Constants.HEARTBEAT_TASK)).collect(toList());
        Set<Event> timbermillEventsSet = new LinkedHashSet<>();
        events.stream().filter(e -> (e.getName() == null) || !e.getName().equals(Constants.HEARTBEAT_TASK)).forEach(e -> timbermillEventsSet.add(e));

        Collection<Event> newlyAdoptedOrphans = getNewlyAdoptedOrphans(timbermillEventsSet);

        if (!heartbeatEvents.isEmpty()) {
            indexHeartbeatEvents(env, heartbeatEvents);
        }

        LOG.info("{} events to handle in current batch", timbermillEventsSet.size());
        Set<String> missingParentEvents = getMissingParentEvents(timbermillEventsSet);

        Map<String, Task> getPreviouslyIndexedParentTasks = new HashMap<>();
        try {
            if (!missingParentEvents.isEmpty()) {
                getPreviouslyIndexedParentTasks = getPreviouslyIndexedParentTasks(timbermillEventsSet, missingParentEvents);

                sendOrphanEventsToCache(timbermillEventsSet);
            }

            long pluginsDuration = applyPlugins(timbermillEventsSet, env);

            List<Event> startEvents = timbermillEventsSet.stream().filter(e -> e.isStartEvent()).collect(toList());
            enrichEvents(getPreviouslyIndexedParentTasks, startEvents, timbermillEventsSet.stream().collect(groupingBy(e -> e.getTaskId())));
            Map<String, Task> tasksMap = getTasksFromEvents(timbermillEventsSet, env);
            if (!newlyAdoptedOrphans.isEmpty()) {
                enrichEvents(tasksMap, newlyAdoptedOrphans, newlyAdoptedOrphans.stream().collect(groupingBy(e -> e.getTaskId())));
                Map<String, Task> orphansTasks = getTasksFromEvents(newlyAdoptedOrphans, env);
                orphansTasks.values().stream().forEach(t -> {
                    cleanNewlyAdpotedTask(t);
                });
                tasksMap.putAll(orphansTasks);
            }
            es.index(tasksMap);
            if (!timbermillEventsSet.isEmpty()) {
                LOG.info("{} tasks were indexed to elasticsearch", timbermillEventsSet.size());
                indexMetadataTask(taskIndexerStartTime, timbermillEventsSet.size(), missingParentEvents.size(), pluginsDuration, env);
            }





            LOG.info("------------------ Batch End ------------------");
        } catch (ElasticsearchException e){
            indexFailedMetadataTask(taskIndexerStartTime, timbermillEventsSet.size(), getPreviouslyIndexedParentTasks.size(), e, env);
        }
    }

    private void cleanNewlyAdpotedTask(Task t) {
        t.setOrphan(false);
        t.setName(null);
        t.setParentId(null);
        t.setStartTime(null);
        t.setEndTime(null);
        t.setDuration(null);
        t.setString(null);
        t.setText(null);
        t.setMetric(null);
        t.setLog(null);
    }

    private Collection<Event> getNewlyAdoptedOrphans(Set<Event> timbermillEvents) {
        Set<String> taskIds = timbermillEvents.stream().filter(e -> e.isStartEvent()).map(e -> e.getTaskId()).collect(toSet());
        Set<Event> adoptedOrphans = new HashSet<>();
        ConcurrentMap<String, Event> cacheMap = cache.asMap();
        for (Map.Entry<String, Event> cacheEntry : cacheMap.entrySet()) {
            if (taskIds.contains(cacheEntry.getValue().getParentId())){
                adoptedOrphans.add(cacheEntry.getValue());
            }
        }
        return adoptedOrphans;
    }

    private void sendOrphanEventsToCache(Collection<Event> timbermillEvents) {
        List<Event> orphanEvents = timbermillEvents.stream().filter(e -> e.isOrphan() != null && e.isOrphan()).collect(toList());
        for (Event orphanEvent : orphanEvents) {

            cache.put(orphanEvent.getTaskId(), orphanEvent);
        }

    }

    private Map<String, Task> getTasksFromEvents(Collection<Event> timbermillEvents, String env) {
        Map<String, Task> tasksMap = new HashMap<>();
        Map<String, List<Event>> eventsMap = timbermillEvents.stream().collect(groupingBy(e -> e.getTaskId()));
        for (Map.Entry<String, List<Event>> eventEntry : eventsMap.entrySet()) {
            Task task = new Task(eventEntry.getValue(), env);
            tasksMap.put(eventEntry.getKey(), task);
        }
        return tasksMap;
    }

    private Map<String, Task> getPreviouslyIndexedParentTasks(Collection<Event> timbermillEvents, Set<String> missingParentTasks) {
        int missingParentNum = missingParentTasks.size();
        LOG.info("{} missing parent events for current batch", missingParentNum);
        Map<String, Task> previouslyIndexedTasks = this.es.fetchIndexedTasks(missingParentTasks);
        if (missingParentNum == previouslyIndexedTasks.size()) {
            LOG.info("All {} missing event were retrieved from elasticsearch..", missingParentNum);
        } else {
            Sets.SetView<String> notFoundEvents = Sets.difference(missingParentTasks, previouslyIndexedTasks.keySet());

            for (String notFoundEvent : notFoundEvents) {
                for (Event event : timbermillEvents) {
                    if (notFoundEvent.equals(event.getParentId())){
                        event.setOrphan(true);
                    }
                    if (notFoundEvent.equals(event.getPrimaryId())){
                        event.setPrimaryId(null);
                    }
                }
            }


            String joinString = StringUtils.join(notFoundEvents, ",");
            LOG.warn("{} missing events were not retrieved from elasticsearch. Events [{}]", missingParentNum - previouslyIndexedTasks.size(), joinString);
        }
        return previouslyIndexedTasks;
    }

    private void indexHeartbeatEvents(String env, List<Event> heartbeatEvents) {
        List<String> heartbeatTasks = heartbeatEvents.stream().map(event -> GSON.toJson(new HeartbeatTask(event))).collect(toList());
        this.es.indexMetaDataEvents(env, heartbeatTasks.toArray(new String[heartbeatTasks.size()]));
    }

    private void enrichEvents(Map<String, Task> previouslyIndexedTasks, Collection<Event> startEvents, Map<String, List<Event>> eventsMap) {
        Map<String, DefaultMutableTreeNode> map = new HashMap<>();
        for (Event startEvent : startEvents) {
            map.put(startEvent.getTaskId(), new DefaultMutableTreeNode(startEvent));
        }
        Set<DefaultMutableTreeNode> roots = new HashSet<>();
        for (Event startEvent : startEvents) {
            DefaultMutableTreeNode parentNode = map.get(startEvent.getParentId());
            if (startEvent.getParentId() != null && parentNode != null) {
                DefaultMutableTreeNode childNode = map.get(startEvent.getTaskId());
                parentNode.add(childNode);
            }
        }

        for (DefaultMutableTreeNode node : map.values()) {
            roots.add((DefaultMutableTreeNode) node.getRoot());
        }

        /*
         * Compute origins and down merge parameters from parent
         */
        for (DefaultMutableTreeNode root : roots) {
            Enumeration enumeration = root.breadthFirstEnumeration();
            while (enumeration.hasMoreElements()){
                DefaultMutableTreeNode curr = (DefaultMutableTreeNode) enumeration.nextElement();
                Event event = (Event) curr.getUserObject();
                List<String> parentsPath = new ArrayList<>();
                String parentId = event.getParentId();
                if (parentId != null) {

                    ParentProperties parentProperties = getParentProperties(previouslyIndexedTasks.get(parentId), eventsMap.get(parentId));

                    String primaryId = parentProperties.getPrimaryId();
                    event.setPrimaryId(primaryId);
                    for (Map.Entry<String, String> entry : parentProperties.getContext().entrySet()) {
                        event.getContext().putIfAbsent(entry.getKey(), entry.getValue());
                    }

                    List<String> parentParentsPath = parentProperties.getParentPath();
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
            }
        }
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
                long duration = endTime.toInstant().toEpochMilli() - startTime.toInstant().toEpochMilli();
                PluginApplierEvent pluginApplierEvent = new PluginApplierEvent(startTime, plugin.getName(), plugin.getClass().getSimpleName(), status, exception, endTime, duration);
                es.indexMetaDataEvents(env, GSON.toJson(pluginApplierEvent));
            }
        } catch (Exception ex) {
            LOG.error("Error running plugins", ex);
        }
        long pluginsEnd = System.currentTimeMillis();
        return pluginsEnd - pluginsStart;
    }

    private void trimAllStrings(List<Event> events) {
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

    private void indexFailedMetadataTask(ZonedDateTime taskIndexerStartTime, Integer eventsAmount,
                                         Integer fetchedAmount, Exception e, String env) {
        ZonedDateTime taskIndexerEndTime = ZonedDateTime.now();
        long duration = taskIndexerEndTime.toInstant().toEpochMilli() - taskIndexerStartTime.toInstant().toEpochMilli();

        IndexEvent indexEvent = new IndexEvent(eventsAmount, fetchedAmount, taskIndexerStartTime, taskIndexerEndTime, duration, TaskStatus.ERROR, ExceptionUtils.getStackTrace(e), null);
        es.indexMetaDataEvents(env, GSON.toJson(indexEvent));
    }

    private void indexMetadataTask(ZonedDateTime taskIndexerStartTime, Integer eventsAmount, Integer fetchedAmount, long pluginsDuration, String env) {

        ZonedDateTime taskIndexerEndTime = ZonedDateTime.now();
        long duration = taskIndexerEndTime.toInstant().toEpochMilli() - taskIndexerStartTime.toInstant().toEpochMilli();

        IndexEvent indexEvent = new IndexEvent(eventsAmount, fetchedAmount, taskIndexerStartTime, taskIndexerEndTime, duration, TaskStatus.SUCCESS, null, pluginsDuration);
        es.indexMetaDataEvents(env, GSON.toJson(indexEvent));
    }

    private static Set<String> getMissingParentEvents(Collection<Event> events) {
        Set<String> allEventParentTasks = events.stream()
                .filter(e -> e.getParentId() != null)
                .map(e -> e.getParentId()).collect(Collectors.toCollection(LinkedHashSet::new));

        return Sets.difference(allEventParentTasks, getAllTaskIds(events));
    }

    private static Set<String> getAllTaskIds(Collection<Event> events) {
        return events.stream().filter(e -> e.isStartEvent()).map(e -> e.getTaskId()).collect(Collectors.toSet());
    }

    private static ParentProperties getParentProperties(Task indexedTask, List<Event> previousEvents) {

        Map<String, String> context = Maps.newHashMap();
        String primaryId = null;
        List<String> parentPath = null;
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
                List<String> previousPath = previousEvent.getParentsPath();
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
        private final List<String> parentPath;
        private final String parentName;

        ParentProperties(String primaryId, Map<String, String> context, List<String> parentPath, String parentName) {
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

        List<String> getParentPath() {
            return parentPath;
        }

        String getParentName() {
            return parentName;
        }
    }
}

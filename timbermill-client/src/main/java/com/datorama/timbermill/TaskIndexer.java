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

    public TaskIndexer(String pluginsJson, Map<String, Integer> propertiesLengthJson, int defaultMaxChars, String elasticUrl, int daysRotation, String awsRegion, int indexBulkSize, int indexingThreads, String elasticUser, String elasticPassword, int maximumCacheSize, int maximumCacheMinutesHold) {
        logPlugins = PluginsConfig.initPluginsFromJson(pluginsJson);
        this.propertiesLengthMap = propertiesLengthJson;
        this.defaultMaxChars = defaultMaxChars;
        this.es = new ElasticsearchClient(elasticUrl, indexBulkSize, daysRotation, awsRegion, indexingThreads, elasticUser, elasticPassword);
        cache = CacheBuilder.newBuilder().maximumSize(maximumCacheSize).expireAfterAccess(maximumCacheMinutesHold, TimeUnit.MINUTES).build();
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
            Map<String, Task> previouslyIndexedParentTasks = new HashMap<>();
            try {
                Set<String> missingParentTaskIds = getMissingParentTaskIds(timbermillEvents, startEventsIds);
                if (!missingParentTaskIds.isEmpty()) {
                    previouslyIndexedParentTasks = this.es.fetchIndexedTasks(missingParentTaskIds);
                    Sets.SetView<String> notFoundEvents = Sets.difference(missingParentTaskIds, previouslyIndexedParentTasks.keySet());
                    if (notFoundEvents.isEmpty()) {
                        LOG.info("All {} missing event were retrieved from elasticsearch..", missingParentTaskIds.size());
                    } else {
                        handleOrphans(timbermillEvents, notFoundEvents);
                    }
                }

                long pluginsDuration = applyPlugins(timbermillEvents, env);

                Map<String, Task> tasksMap = getEnrichedTasksFromEvents(env, timbermillEvents, previouslyIndexedParentTasks);
                Collection<Event> newlyAdoptedOrphans = getAdoptedOrphans(startEventsIds);

                if (!newlyAdoptedOrphans.isEmpty()) {
                    Map<String, Task> orphansTasks = getEnrichedTasksFromEvents(env, newlyAdoptedOrphans, tasksMap);
                    orphansTasks.values().forEach(t -> cleanAdoptedTask(t));
                    tasksMap.putAll(orphansTasks);
                }

                es.index(tasksMap);

                LOG.info("{} tasks were indexed to elasticsearch", timbermillEvents.size());
                indexMetadataTask(taskIndexerStartTime, timbermillEvents.size(), missingParentTaskIds.size(), pluginsDuration, env);
            } catch (ElasticsearchException e) {
                indexFailedMetadataTask(taskIndexerStartTime, timbermillEvents.size(), previouslyIndexedParentTasks.size(), e, env);
            }
        }
        LOG.info("------------------ Batch End ------------------");
    }

    private Map<String, Task> getEnrichedTasksFromEvents(String env, Collection<Event> timbermillEvents, Map<String, Task> previouslyIndexedParentTasks) {
        Map<String, List<Event>> eventsMap = timbermillEvents.stream().collect(groupingBy(e -> e.getTaskId()));
        enrichEvents(timbermillEvents, eventsMap, previouslyIndexedParentTasks);
        return getTasksFromEvents(env, eventsMap);
    }

    private void handleOrphans(Collection<Event> timbermillEvents, Sets.SetView<String> notFoundEvents) {
        timbermillEvents.forEach(event -> {
            if (notFoundEvents.contains(event.getParentId())){
                event.setOrphan(true);
                cache.put(event.getTaskId(), event);
            }
        });
        String joinString = StringUtils.join(notFoundEvents, ",");
        LOG.warn("{} missing events were not retrieved from elasticsearch. Events that were added to orphans queue [{}]", notFoundEvents.size(), joinString);
    }

    private void cleanAdoptedTask(Task t) {
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

    private Collection<Event> getAdoptedOrphans(Collection<String> startEventsId) {
        Collection<Event> adoptedOrphans = new HashSet<>();
        ConcurrentMap<String, Event> cacheMap = cache.asMap();
        for (Map.Entry<String, Event> cacheEntry : cacheMap.entrySet()) {
            Event adoptedOrphan = cacheEntry.getValue();
            String orphansParentId = adoptedOrphan.getParentId();
            if (startEventsId.contains(orphansParentId)){
                adoptedOrphans.add(adoptedOrphan);
                cache.invalidate(cacheEntry.getKey());
            }
        }
        return adoptedOrphans;
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

    private void enrichEvents(Collection<Event> timbermillEvents, Map<String, List<Event>> eventsMap, Map<String, Task> previouslyIndexedTasks) {
        Collection<DefaultMutableTreeNode> roots = getTreesRoots(timbermillEvents);

        /*
         * Compute origins and down merge parameters from parent
         */
        for (DefaultMutableTreeNode root : roots) {
            Enumeration enumeration = root.breadthFirstEnumeration();
            while (enumeration.hasMoreElements()){
                DefaultMutableTreeNode curr = (DefaultMutableTreeNode) enumeration.nextElement();
                Event event = (Event) curr.getUserObject();
                enrichEvent(event, eventsMap, previouslyIndexedTasks);
            }
        }
    }

    private void enrichEvent(Event event, Map<String, List<Event>> eventsMap, Map<String, Task> previouslyIndexedTasks) {
        List<String> parentsPath = new ArrayList<>();
        String parentId = event.getParentId();
        if (parentId != null) {

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
    }

    private Collection<DefaultMutableTreeNode> getTreesRoots(Collection<Event> timbermillEvents) {
        Map<String, Event> nodesMap = new HashMap<>();
        for (Event timbermillEvent : timbermillEvents) {
            if (timbermillEvent.isStartEvent()) {
                nodesMap.put(timbermillEvent.getTaskId(), timbermillEvent);
            }
        }
        Collection<DefaultMutableTreeNode> nodes = new HashSet<>();
        for (Event startEvent : nodesMap.values()) {
            Event child = nodesMap.get(startEvent.getTaskId());
            DefaultMutableTreeNode childNode = new DefaultMutableTreeNode(child);
            nodes.add(childNode);

            if (startEvent.getParentId() != null) {
                Event parent = nodesMap.get(startEvent.getParentId());
                if (parent != null) {
                    DefaultMutableTreeNode parentNode = new DefaultMutableTreeNode(parent);
                    parentNode.add(childNode);
                }
            }
        }

        Collection<DefaultMutableTreeNode> roots = new HashSet<>();
        for (DefaultMutableTreeNode node : nodes) {
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

    private static Set<String> getMissingParentTaskIds(Collection<Event> timbermillEvents, Collection<String> startEventsIds) {

        return timbermillEvents.stream()
                .filter(e -> e.getParentId() != null && !startEventsIds.contains(e.getParentId()))
                .map(e -> e.getParentId())
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

        else if (previousEvents != null){
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

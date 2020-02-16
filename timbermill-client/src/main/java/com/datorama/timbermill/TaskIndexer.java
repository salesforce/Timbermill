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

public class TaskIndexer {

    private static final Logger LOG = LoggerFactory.getLogger(TaskIndexer.class);
    private static final String TEXT = "text";
    private static final String STRING = "string";
    private static final String CTX = "ctx";
    static final int MAX_CHARS_ALLOWED_FOR_NON_ANALYZED_FIELDS = 32765;
    static final int MAX_CHARS_ALLOWED_FOR_ANALYZED_FIELDS = 1000000;

    private final ElasticsearchClient es;
    private final Collection<TaskLogPlugin> logPlugins;
    private final Cache<String, Queue<Event>> parentIdTORootOrphansEventsCache;
    private long daysRotation;

    public TaskIndexer(ElasticsearchParams elasticsearchParams, ElasticsearchClient es) {
        this.daysRotation = elasticsearchParams.getDaysRotation() < 0 ? 1 : elasticsearchParams.getDaysRotation();
        this.logPlugins = PluginsConfig.initPluginsFromJson(elasticsearchParams.getPluginsJson());
        this.es = es;
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
            IndexEvent indexEvent = handleTimbermillEvents(env, taskIndexerStartTime, timbermillEvents, startEventsIds);
            es.indexMetaDataTasks(env, GSON.toJson(indexEvent));
        }
        LOG.info("------------------ Batch End ------------------");
    }

    private IndexEvent handleTimbermillEvents(String env, ZonedDateTime taskIndexerStartTime, Collection<Event> timbermillEvents, Collection<String> startEventsIds) {
        Map<String, Task> previouslyIndexedParentTasks = fetchPreviouslyIndexedParentTasks(timbermillEvents, startEventsIds);
        applyPlugins(timbermillEvents, env);
        Map<String, Task> tasksMap = createEnrichedTasks(timbermillEvents, previouslyIndexedParentTasks);
        String index = es.createTimbermillAlias(env);
        es.index(tasksMap, index);
        es.rolloverIndex(index);
        LOG.info("{} tasks were indexed to elasticsearch", timbermillEvents.size());
        return new IndexEvent(env, previouslyIndexedParentTasks.size(), taskIndexerStartTime, ZonedDateTime.now(), timbermillEvents.size(), daysRotation);
    }

    public static long getTimesDuration(ZonedDateTime taskIndexerStartTime, ZonedDateTime taskIndexerEndTime) {
        return ChronoUnit.MILLIS.between(taskIndexerStartTime, taskIndexerEndTime);
    }

    private Map<String, Task> fetchPreviouslyIndexedParentTasks(Collection<Event> timbermillEvents, Collection<String> startEventsIds) {
        String[] missingParentTaskIds = getMissingParentTaskIds(timbermillEvents, startEventsIds);
        try {
            return this.es.fetchIndexedTasks(missingParentTaskIds);
        } catch (Throwable t) {
            LOG.error("Error fetching indexed tasks from Elasticsearch", t);
            return Maps.newHashMap();
        }
    }

    private Map<String, Task> createEnrichedTasks(Collection<Event> timbermillEvents, Map<String, Task> previouslyIndexedParentTasks) {
        Map<String, List<Event>> eventsMap = timbermillEvents.stream().collect(groupingBy(Event::getTaskId));
        Collection<DefaultMutableTreeNode> roots = getTreesRoots(timbermillEvents);
        enrichEvents(roots, eventsMap, previouslyIndexedParentTasks);
        return getTasksFromEvents(eventsMap);
    }

    private Map<String, Task> getTasksFromEvents(Map<String, List<Event>> eventsMap) {
        Map<String, Task> tasksMap = new HashMap<>();
        for (Map.Entry<String, List<Event>> eventEntry : eventsMap.entrySet()) {
            Task task = new Task(eventEntry.getValue(), daysRotation);
            tasksMap.put(eventEntry.getKey(), task);
        }
        return tasksMap;
    }

    private void indexHeartbeatEvents(String env, Collection<Event> heartbeatEvents) {
        String[] metadataEvents = heartbeatEvents.stream().map(event -> GSON.toJson(new HeartbeatTask(event, daysRotation))).toArray(String[]::new);
        this.es.indexMetaDataTasks(env, metadataEvents);
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
                if (event.getContext() == null){
                    event.setContext(Maps.newHashMap());
                }
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
                if (eventsMap.get(parentId).stream().anyMatch(e -> e instanceof StartEvent)){
                    Boolean isOrphan = event.isOrphan();
                    return isOrphan != null && isOrphan;
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


    private void applyPlugins(Collection<Event> events, String env) {
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
                PluginApplierTask pluginApplierTask = new PluginApplierTask(env, plugin.getName(), plugin.getClass().getSimpleName(), status, exception, endTime, duration, startTime, daysRotation);
                es.indexMetaDataTasks(env, GSON.toJson(pluginApplierTask));
            }
        } catch (Throwable t) {
            LOG.error("Error running plugins", t);
        }
    }

    private void trimAllStrings(Collection<Event> events) {
        events.forEach(e -> {
            e.setStrings(getTrimmedLongValues(e.getStrings(), STRING, e));
            e.setContext(getTrimmedLongValues(e.getContext(), CTX, e));
            e.setText(getTrimmedLongValues(e.getText(), TEXT, e));
            e.setMetrics(removeNaNs(e));
        });
    }

    private Map<String, Number> removeNaNs(Event event) {
        Map<String, Number> metrics = event.getMetrics();
        Map<String, Number> newMetrics = Maps.newHashMap();
        if (metrics != null) {
            for (Map.Entry<String, Number> entry : metrics.entrySet()) {
                Number value = entry.getValue();
                String key = entry.getKey();
                if (value != null) {
					if (Double.isNaN(value.doubleValue()) || Float.isNaN(value.floatValue())) {
						newMetrics.put(key, 0);
						LOG.error("NaN value for key {} in ID {}. Changed to 0", key, event.getTaskId());
					} else {
						newMetrics.put(key, value);
					}
				}
            }
        }
        return newMetrics;
    }

    private Map<String, String> getTrimmedLongValues(Map<String, String> oldMap, String type, Event event) {
        Map<String, String> newMap = new HashMap<>();
        if (oldMap != null) {
            for (Map.Entry<String, String> entry : oldMap.entrySet()) {
                String key = entry.getKey().replace(".", "_");
                String value = trimIfNeededValue(type, key, entry.getValue(), event);
                newMap.put(key, value);
            }
        }
        return newMap;
    }

    private String trimIfNeededValue(String type, String key, String value, Event event) {
        if (type.equals(TEXT)) {
            value = trimValue(type, key, value, event, MAX_CHARS_ALLOWED_FOR_ANALYZED_FIELDS);
        } else {
            value = trimValue(type, key, value, event, MAX_CHARS_ALLOWED_FOR_NON_ANALYZED_FIELDS);
        }
        return value;
    }

    private String trimValue(String type, String key, String value, Event event, int maxChars) {
        if (value.length() > maxChars) {
            LOG.warn("Value starting with {} key {} under ID {} is  too long, trimmed to {} characters.", value.substring(0, 20), type + '.' + key, event.getTaskId(), maxChars);
            value = value.substring(0, maxChars);
        }
        return value;
    }

    private static String[] getMissingParentTaskIds(Collection<Event> timbermillEvents, Collection<String> startEventsIds) {

        return timbermillEvents.stream()
                .filter(e -> e.getParentId() != null && !startEventsIds.contains(e.getParentId()))
                .map(Event::getParentId).toArray(String[]::new);
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

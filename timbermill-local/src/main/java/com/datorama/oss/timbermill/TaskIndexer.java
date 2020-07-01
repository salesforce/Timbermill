package com.datorama.oss.timbermill;

import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.swing.tree.DefaultMutableTreeNode;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.oss.timbermill.common.Constants;
import com.datorama.oss.timbermill.common.ElasticsearchUtil;
import com.datorama.oss.timbermill.plugins.PluginsConfig;
import com.datorama.oss.timbermill.plugins.TaskLogPlugin;
import com.datorama.oss.timbermill.unit.*;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import kamon.Kamon;
import kamon.metric.Metric;
import static com.datorama.oss.timbermill.common.Constants.GSON;

public class TaskIndexer {

    private static final Logger LOG = LoggerFactory.getLogger(TaskIndexer.class);

    private final ElasticsearchClient es;
    private final Collection<TaskLogPlugin> logPlugins;
    private final Cache<String, Queue<AdoptedEvent>> parentIdTORootOrphansEventsCache;
    private Metric.Histogram tasksFetchedHistogram = Kamon.histogram("timbermill2.tasks.fetched.histogram");
    private Metric.Histogram tasksIndexedHistogram = Kamon.histogram("timbermill2.tasks.indexed.histogram");
    private Metric.Histogram batchDurationHistogram = Kamon.histogram("timbermill2.batch.duration.histogram");
    private long daysRotation;

    public TaskIndexer(ElasticsearchParams elasticsearchParams, ElasticsearchClient es) {
        this.daysRotation = Math.max(elasticsearchParams.getDaysRotation(), 1);
        this.logPlugins = PluginsConfig.initPluginsFromJson(elasticsearchParams.getPluginsJson());
        this.es = es;
        CacheBuilder<String, Queue<AdoptedEvent>> cacheBuilder = CacheBuilder.newBuilder().weigher((key, value) -> {
            int sum = value.stream().mapToInt(Event::estimatedSize).sum();
            return key.length() + sum;
        });
        parentIdTORootOrphansEventsCache = cacheBuilder
                .maximumWeight(elasticsearchParams.getMaximumCacheSize()).expireAfterWrite(elasticsearchParams.getMaximumCacheMinutesHold(), TimeUnit.MINUTES).removalListener(
                        notification -> {
                            if (notification.wasEvicted()){
                                LOG.warn("Event {} was evicted from the cache due to {}", notification.getKey(), notification.getCause());
                            }
                        }).build();
    }

    public void retrieveAndIndex(Collection<Event> events, String env) {
        LOG.info("------------------ Batch Start ------------------");
        ZonedDateTime taskIndexerStartTime = ZonedDateTime.now();

        Collection<String> heartbeatEvents = new HashSet<>();
        Collection<Event> timbermillEvents = new LinkedHashSet<>();

        events.forEach(e -> {
            if (e.getName() != null && e.getName().equals(Constants.HEARTBEAT_TASK)){
                String heartbeatJson = GSON.toJson(new HeartbeatTask(e, daysRotation));
                heartbeatEvents.add(heartbeatJson);
            }
            else{
                if (e.getTaskId() == null){
                    LOG.warn("Task ID is null for event {}", GSON.toJson(e));
                }
                else {
                    e.trimAllStrings();
                    timbermillEvents.add(e);
                }
            }
        });

        if (!heartbeatEvents.isEmpty()) {
            this.es.indexMetaDataTasks(env, heartbeatEvents);
        }

        LOG.info("{} events to handle in current batch", timbermillEvents.size());

        if (!timbermillEvents.isEmpty()) {
            handleTimbermillEvents(env, taskIndexerStartTime, timbermillEvents);
        }
        LOG.info("------------------ Batch End ------------------");
    }

    private void handleTimbermillEvents(String env, ZonedDateTime taskIndexerStartTime, Collection<Event> timbermillEvents) {
        applyPlugins(timbermillEvents, env);

        Map<String, DefaultMutableTreeNode> nodesMap = Maps.newHashMap();
        Set<String> startEventsIds = Sets.newHashSet();
        Set<String> parentIds = Sets.newHashSet();
        Map<String, List<Event>> eventsMap = Maps.newHashMap();
        populateCollections(timbermillEvents, nodesMap, startEventsIds, parentIds, eventsMap);
        Map<String, Task> previouslyIndexedParentTasks = getMissingParents(startEventsIds, parentIds);
        connectNodesByParentId(nodesMap);

        Map<String, Task> tasksMap = createEnrichedTasks(nodesMap, eventsMap, previouslyIndexedParentTasks);

        String index = es.createTimbermillAlias(env);
        es.index(tasksMap, index);
        es.rolloverIndex(index);
        LOG.info("{} tasks were indexed to elasticsearch", tasksMap.size());
        reportBatchMetrics(env, previouslyIndexedParentTasks.size(), taskIndexerStartTime, timbermillEvents.size());
    }

    private void reportBatchMetrics(String env, int tasksFetchedSize, ZonedDateTime taskIndexerStartTime, int indexedTasksSize) {
        ZonedDateTime now = ZonedDateTime.now();
        long timesDuration = ElasticsearchUtil.getTimesDuration(taskIndexerStartTime, now);
        reportToElasticsearch(env, tasksFetchedSize, taskIndexerStartTime, indexedTasksSize, timesDuration, now);
        reportToKamon(tasksFetchedSize, indexedTasksSize, timesDuration);
    }

    private void reportToKamon(int tasksFetchedSize, int indexedTasksSize, long duration) {
        tasksFetchedHistogram.withoutTags().record(tasksFetchedSize);
        tasksIndexedHistogram.withoutTags().record(indexedTasksSize);
        batchDurationHistogram.withoutTags().record(duration);
    }

    private void reportToElasticsearch(String env, int tasksFetchedSize, ZonedDateTime taskIndexerStartTime, int indexedTasksSize, long timesDuration, ZonedDateTime now) {
        IndexEvent indexEvent = new IndexEvent(env, tasksFetchedSize, taskIndexerStartTime, now, indexedTasksSize,  daysRotation,
                timesDuration);
        es.indexMetaDataTasks(env, Lists.newArrayList(GSON.toJson(indexEvent)));
    }

    private void populateCollections(Collection<Event> timbermillEvents, Map<String, DefaultMutableTreeNode> nodesMap, Set<String> startEventsIds, Set<String> parentIds,
            Map<String, List<Event>> eventsMap) {
        timbermillEvents.forEach(event -> {
            if (event.isStartEvent()){
                startEventsIds.add(event.getTaskId());

                nodesMap.put(event.getTaskId(), new DefaultMutableTreeNode(event));
            }
            if (event.getParentId() != null){
                parentIds.add(event.getParentId());
            }

            if (!eventsMap.containsKey(event.getTaskId())){
                eventsMap.put(event.getTaskId(), Lists.newArrayList(event));
            }
            else {
                List<Event> events = eventsMap.get(event.getTaskId());
                events.add(event);
            }


        });
    }

    private Map<String, Task> getMissingParents(Set<String> startEventsIds, Set<String> parentIds) {
        parentIds.removeAll(startEventsIds);
        LOG.info("Fetching {} missing parents", parentIds.size());
        Map<String, Task> previouslyIndexedParentTasks = Maps.newHashMap();
        try {
            previouslyIndexedParentTasks = this.es.fetchIndexedTasks(parentIds);
        } catch (Throwable t) {
            LOG.error("Error fetching indexed tasks from Elasticsearch", t);
        }
        LOG.info("Fetched {} parents", previouslyIndexedParentTasks.size());
        return previouslyIndexedParentTasks;
    }

    private void connectNodesByParentId(Map<String, DefaultMutableTreeNode> nodesMap) {
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
    }

    private Map<String, Task> createEnrichedTasks(Map<String, DefaultMutableTreeNode> nodesMap, Map<String, List<Event>> eventsMap,
            Map<String, Task> previouslyIndexedParentTasks) {
        enrichStartEventsByOrder(nodesMap.values(), eventsMap, previouslyIndexedParentTasks);
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

    private void enrichStartEventsByOrder(Collection<DefaultMutableTreeNode> nodes, Map<String, List<Event>> eventsMap, Map<String, Task> previouslyIndexedTasks) {
        /*
         * Compute origins and down merge parameters from parent
         */
        for (DefaultMutableTreeNode node : nodes) {
            if (node.isRoot()) {
                Enumeration enumeration = node.breadthFirstEnumeration();
                while (enumeration.hasMoreElements()) {
                    DefaultMutableTreeNode curr = (DefaultMutableTreeNode) enumeration.nextElement();
                    Event startEvent = (Event) curr.getUserObject();
                    enrichStartEvent(eventsMap, previouslyIndexedTasks, startEvent);
                }
            }
        }
    }

    public static void logErrorInEventsMap(Map<String, List<Event>> eventsMap, String where) {
        for (Map.Entry<String, List<Event>> stringListEntry : eventsMap.entrySet()) {
            List<Event> value = stringListEntry.getValue();
            if (value.stream().filter(Event::isStartEvent).count() > 1){
                LOG.warn("Too many start events in {} events: {}" ,where , GSON.toJson(value));
            }
        }
    }

    private void updateAdoptedOrphans(Map<String, List<Event>> eventsMap, String parentTaskId) {
        Queue<AdoptedEvent> adoptedEvents = parentIdTORootOrphansEventsCache.getIfPresent(parentTaskId);

        if (adoptedEvents != null) {
            parentIdTORootOrphansEventsCache.invalidate(parentTaskId);
            for (AdoptedEvent adoptedEvent : adoptedEvents) {
                populateParentParams(adoptedEvent, null, eventsMap.get(parentTaskId));
                String adoptedId = adoptedEvent.getTaskId();
                if (eventsMap.containsKey(adoptedId)){
                    eventsMap.get(adoptedId).add(adoptedEvent);
                }
                else{
                    eventsMap.put(adoptedId, Lists.newArrayList(adoptedEvent));
                }
                updateAdoptedOrphans(eventsMap, adoptedId);
            }
        }

    }

    private void enrichStartEvent(Map<String, List<Event>> eventsMap, Map<String, Task> previouslyIndexedTasks, Event startEvent) {
        String parentId = startEvent.getParentId();
        if (parentId != null) {
            if (isOrphan(startEvent, previouslyIndexedTasks, eventsMap)){
                startEvent.setOrphan(true);
                startEvent.setPrimaryId(null);
                addOrphanToCache(startEvent, parentId);
            }
            else {
                populateParentParams(startEvent, previouslyIndexedTasks.get(parentId), eventsMap.get(parentId));
            }
        }
        else{
            startEvent.setPrimaryId(startEvent.getTaskId());
        }
        if (hasAdoptedOrphans(startEvent)) {
            updateAdoptedOrphans(eventsMap, startEvent.getTaskId());
        }
    }

    private boolean hasAdoptedOrphans(Event event) {
        String taskId = event.getTaskId();
        Queue<AdoptedEvent> orphansEvents = parentIdTORootOrphansEventsCache.getIfPresent(taskId);
        return orphansEvents != null && (event.isOrphan() == null || !event.isOrphan());
    }

    private void populateParentParams(Event event, Task parentIndexedTask, Collection<Event> parentCurrentEvent) {
        ParentProperties parentProperties = getParentProperties(parentIndexedTask, parentCurrentEvent);
        List<String> parentsPath =  new ArrayList<>();
        String primaryId = parentProperties.getPrimaryId();
        event.setPrimaryId(primaryId);
        if (event.getContext() == null){
            event.setContext(Maps.newHashMap());
        }
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

        if(!parentsPath.isEmpty()) {
            event.setParentsPath(parentsPath);
        }
    }

    private void addOrphanToCache(Event startEvent, String parentId) {
        Queue<AdoptedEvent> eventList = parentIdTORootOrphansEventsCache.getIfPresent(parentId);

        AdoptedEvent orphanEvent = new AdoptedEvent(startEvent);
        if (eventList == null) {
            eventList = new LinkedList<>();
            eventList.add(orphanEvent);
            parentIdTORootOrphansEventsCache.put(parentId, eventList);
        } else {
            eventList.add(orphanEvent);
        }
    }

    private boolean isOrphan(Event event, Map<String, Task> previouslyIndexedTasks, Map<String, List<Event>> eventsMap) {
        String parentId = event.getParentId();
        if (parentId == null) {
            return false;
        } else {
            if (previouslyIndexedTasks.containsKey(parentId)){
                return false;
            }
            if (eventsMap.containsKey(parentId)){
                if (eventsMap.get(parentId).stream().anyMatch(Event::isAdoptedEvent)) {
                    return false;
                }
                if (eventsMap.get(parentId).stream().anyMatch(Event::isStartEvent)){
                    List<Event> parentEvents = eventsMap.get(parentId).stream().filter(Event::isStartEvent).collect(Collectors.toList());
                    if (parentEvents.size() != 1){
                        LOG.warn("Problem with parent events. Events: {}", GSON.toJson(parentEvents));
                    }
                    for (Event e : parentEvents) {
                        if (e.isOrphan() != null && e.isOrphan()){
                            return true;
                        }
                    }
                    return false;
                }
            }
            return true;
        }
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
                long duration = ElasticsearchUtil.getTimesDuration(startTime, endTime);
                PluginApplierTask pluginApplierTask = new PluginApplierTask(env, plugin.getName(), plugin.getClass().getSimpleName(), status, exception, endTime, duration, startTime, daysRotation);
                es.indexMetaDataTasks(env, Lists.newArrayList(GSON.toJson(pluginApplierTask)));
            }
        } catch (Throwable t) {
            LOG.error("Error running plugins", t);
        }
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

    public Cache<String, Queue<AdoptedEvent>> getParentIdTORootOrphansEventsCache() {
        return parentIdTORootOrphansEventsCache;
    }
}

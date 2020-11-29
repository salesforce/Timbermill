package com.datorama.oss.timbermill;

import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

import javax.swing.tree.DefaultMutableTreeNode;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.oss.timbermill.common.Constants;
import com.datorama.oss.timbermill.common.ElasticsearchUtil;
import com.datorama.oss.timbermill.common.KamonConstants;
import com.datorama.oss.timbermill.plugins.PluginsConfig;
import com.datorama.oss.timbermill.plugins.TaskLogPlugin;
import com.datorama.oss.timbermill.unit.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import static com.datorama.oss.timbermill.ElasticsearchClient.GSON;
import static com.datorama.oss.timbermill.common.ElasticsearchUtil.getOldAlias;
import static com.datorama.oss.timbermill.common.ElasticsearchUtil.getTimbermillIndexAlias;

public class TaskIndexer {

    private static final Logger LOG = LoggerFactory.getLogger(TaskIndexer.class);
    public static final String FLOW_ID_LOG = "Flow ID: [{}]";

    private final ElasticsearchClient es;
    private final Collection<TaskLogPlugin> logPlugins;
    private long daysRotation;
    private String timbermillVersion;

    public TaskIndexer(String pluginsJson, Integer daysRotation, ElasticsearchClient es, String timbermillVersion) {

        this.daysRotation = calculateDaysRotation(daysRotation);
        this.logPlugins = PluginsConfig.initPluginsFromJson(pluginsJson);
        this.es = es;
        this.timbermillVersion = timbermillVersion;
    }

    private static int calculateDaysRotation(int daysRotationParam) {
        return Math.max(daysRotationParam, 1);
    }

    public void close() {
        es.close();
    }

    public void retrieveAndIndex(Collection<Event> events, String env) {
        String flowId = "Task Indexer - " + UUID.randomUUID().toString();
        LOG.info(FLOW_ID_LOG + " #### Batch Start ####", flowId);
        ZonedDateTime taskIndexerStartTime = ZonedDateTime.now();
        LOG.info(FLOW_ID_LOG + " {} events to be handled in current batch", flowId, events.size());

        Collection<String> heartbeatEvents = new HashSet<>();
        Collection<Event> timbermillEvents = new LinkedHashSet<>();

        events.forEach(e -> {
            if (e.getName() != null && e.getName().equals(Constants.HEARTBEAT_TASK)){
                String heartbeatJson = GSON.toJson(new HeartbeatTask(e, daysRotation));
                heartbeatEvents.add(heartbeatJson);
            }
            else{
                if (e.getTaskId() == null){
                    LOG.warn(FLOW_ID_LOG + " Task ID is null for event {}", flowId, GSON.toJson(e));
                }
                else {
                    e.fixErrors();
                    e.trimAllStrings();
                    timbermillEvents.add(e);
                }
            }
        });

        if (!heartbeatEvents.isEmpty()) {
            this.es.indexMetaDataTasks(env, heartbeatEvents, flowId);
        }

        if (!timbermillEvents.isEmpty()) {
            handleTimbermillEvents(env, taskIndexerStartTime, timbermillEvents, flowId);
        }
        LOG.info(FLOW_ID_LOG + " #### Batch End ####", flowId);
    }

    private void handleTimbermillEvents(String env, ZonedDateTime taskIndexerStartTime, Collection<Event> timbermillEvents, String flowId) {
        applyPlugins(timbermillEvents, env, flowId);

        Map<String, DefaultMutableTreeNode> nodesMap = Maps.newHashMap();
        Set<String> startEventsIds = Sets.newHashSet();
        Set<String> parentIds = Sets.newHashSet();
        Map<String, List<Event>> eventsMap = Maps.newHashMap();
        populateCollections(timbermillEvents, nodesMap, startEventsIds, parentIds, eventsMap);

        String currentAlias = getTimbermillIndexAlias(env);
        String oldAlias = getOldAlias(currentAlias);
        boolean oldAliasExists;
        try {
            oldAliasExists = es.isAliasExists(flowId, oldAlias);
        } catch (MaxRetriesException e) {
            oldAliasExists = false;
        }
        Map<String, Task> previouslyIndexedParentTasks;
        if (oldAliasExists){
            previouslyIndexedParentTasks = es.getMissingParents(startEventsIds, parentIds, flowId, currentAlias, oldAlias);
        }
        else{
            previouslyIndexedParentTasks = es.getMissingParents(startEventsIds, parentIds, flowId, currentAlias);
        }
        connectNodesByParentId(nodesMap);

        Map<String, Task> tasksMap = createEnrichedTasks(nodesMap, eventsMap, previouslyIndexedParentTasks);

        String index = es.createTimbermillAlias(env, flowId);
        es.index(tasksMap, index, flowId);
        if (!index.endsWith(ElasticsearchUtil.getIndexSerial(1))){
            es.rolloverIndex(index, flowId);
        }
        LOG.info(FLOW_ID_LOG + " {} tasks were indexed to elasticsearch", flowId, tasksMap.size());
        reportBatchMetrics(env, previouslyIndexedParentTasks.size(), taskIndexerStartTime, timbermillEvents.size(), flowId);
    }

    private void reportBatchMetrics(String env, int tasksFetchedSize, ZonedDateTime taskIndexerStartTime, int indexedTasksSize, String flowId) {
        ZonedDateTime now = ZonedDateTime.now();
        long timesDuration = ElasticsearchUtil.getTimesDuration(taskIndexerStartTime, now);
        reportToElasticsearch(env, tasksFetchedSize, taskIndexerStartTime, indexedTasksSize, timesDuration, now, flowId);
        reportToKamon(tasksFetchedSize, indexedTasksSize, timesDuration);
    }

    private void reportToKamon(int tasksFetchedSize, int indexedTasksSize, long duration) {
        KamonConstants.MISSING_PARENTS_TASKS_FETCHED_HISTOGRAM.withoutTags().record(tasksFetchedSize);
        KamonConstants.TASKS_INDEXED_HISTOGRAM.withoutTags().record(indexedTasksSize);
        KamonConstants.BATCH_DURATION_HISTOGRAM.withoutTags().record(duration);
    }

    private void reportToElasticsearch(String env, int tasksFetchedSize, ZonedDateTime taskIndexerStartTime, int indexedTasksSize, long timesDuration, ZonedDateTime now, String flowId) {
        IndexEvent indexEvent = new IndexEvent(env, tasksFetchedSize, taskIndexerStartTime, now, indexedTasksSize,  daysRotation,
                timesDuration);
        es.indexMetaDataTasks(env, Lists.newArrayList(GSON.toJson(indexEvent)), flowId);
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
        return getTasksFromEvents(eventsMap, daysRotation, timbermillVersion);
    }

    public static Map<String, Task> getTasksFromEvents(Map<String, List<Event>> eventsMap, long daysRotation, String timbermillVersion) {
        Map<String, Task> tasksMap = new HashMap<>();
        for (Map.Entry<String, List<Event>> eventEntry : eventsMap.entrySet()) {
            Task task = new Task(eventEntry.getValue(), daysRotation, timbermillVersion);
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

    private void enrichStartEvent(Map<String, List<Event>> eventsMap, Map<String, Task> previouslyIndexedTasks, Event startEvent) {
        String parentId = startEvent.getParentId();
        if (parentId != null) {
            if (isOrphan(startEvent, previouslyIndexedTasks, eventsMap)){
                startEvent.setOrphan(true);
                startEvent.setPrimaryId(null);
            }
            else {
                populateParentParams(startEvent, previouslyIndexedTasks.get(parentId), eventsMap.get(parentId));
            }
        }
        else{
            startEvent.setPrimaryId(startEvent.getTaskId());
        }
    }

    public static void populateParentParams(Event event, Task parentIndexedTask, Collection<Event> parentCurrentEvent) {
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
                        LOG.warn("Too many parents found for parent ID [{}] child task ID [{}] Events: {}", parentId, event.getTaskId(), GSON.toJson(parentEvents));
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

    private void applyPlugins(Collection<Event> events, String env, String flowId) {
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
                es.indexMetaDataTasks(env, Lists.newArrayList(GSON.toJson(pluginApplierTask)), flowId);
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

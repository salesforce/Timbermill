package com.datorama.oss.timbermill;

import com.datorama.oss.timbermill.common.Constants;
import com.datorama.oss.timbermill.common.ElasticsearchUtil;
import com.datorama.oss.timbermill.common.KamonConstants;
import com.datorama.oss.timbermill.common.cache.AbstractCacheHandler;
import com.datorama.oss.timbermill.common.cache.CacheHandlerUtil;
import com.datorama.oss.timbermill.plugins.PluginsConfig;
import com.datorama.oss.timbermill.plugins.TaskLogPlugin;
import com.datorama.oss.timbermill.unit.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import kamon.metric.Timer;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.ThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.tree.DefaultMutableTreeNode;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static com.datorama.oss.timbermill.ElasticsearchClient.GSON;
import static com.datorama.oss.timbermill.ParentResolver.populateParentParams;

public class TaskIndexer {

    private static final Logger LOG = LoggerFactory.getLogger(TaskIndexer.class);

    private final ElasticsearchClient es;
    private final Collection<TaskLogPlugin> logPlugins;
    private AbstractCacheHandler cacheHandler;
    private long daysRotation;
    private String timbermillVersion;

    public TaskIndexer(String pluginsJson, Integer daysRotation, ElasticsearchClient es, String timbermillVersion,
                       long maximumOrphansCacheWeight, long maximumTasksCacheWeight,
                       String cacheStrategy, String redisHost, int redisPort, String redisPass, String redisMaxMemory,
                       String redisMaxMemoryPolicy, boolean redisUseSsl, int redisTtlInSeconds, int redisGetSize) {

        this.daysRotation = calculateDaysRotation(daysRotation);
        this.logPlugins = PluginsConfig.initPluginsFromJson(pluginsJson);
        this.es = es;
        this.timbermillVersion = timbermillVersion;
        cacheHandler = CacheHandlerUtil.getCacheHandler(cacheStrategy, maximumTasksCacheWeight, maximumOrphansCacheWeight,
                redisHost, redisPort, redisPass, redisMaxMemory, redisMaxMemoryPolicy, redisUseSsl, redisTtlInSeconds, redisGetSize);
    }

    private static int calculateDaysRotation(int daysRotationParam) {
        return Math.max(daysRotationParam, 1);
    }

    public void close() {
        es.close();
        cacheHandler.close();
    }

    public void retrieveAndIndex(Collection<Event> events, String env) {
        String flowId = "Task Indexer - " + UUID.randomUUID().toString();
        ThreadContext.put("id", flowId);
        LOG.info("#### Batch Start ####");
        Timer.Started start = KamonConstants.BATCH_DURATION_TIMER.withoutTags().start();
        ZonedDateTime taskIndexerStartTime = ZonedDateTime.now();
        LOG.info("{} events to be handled in current batch", events.size());

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
                    e.fixErrors();
                    e.replaceAllFieldsWithDots();
                    e.trimAllStrings();
                    timbermillEvents.add(e);
                }
            }
        });

        if (!heartbeatEvents.isEmpty()) {
            this.es.indexMetaDataTasks(env, heartbeatEvents);
        }

        if (!timbermillEvents.isEmpty()) {
            int previouslyIndexedParentSize = handleTimbermillEvents(env, timbermillEvents);
            reportBatchMetrics(env, previouslyIndexedParentSize, taskIndexerStartTime, timbermillEvents.size());
        }
        start.stop();
        LOG.info("#### Batch End ####");
    }

    private int handleTimbermillEvents(String env, Collection<Event> timbermillEvents) {
        applyPlugins(timbermillEvents, env);

        Map<String, DefaultMutableTreeNode> nodesMap = Maps.newHashMap();
        Set<String> startEventsIds = Sets.newHashSet();
        Set<String> parentIds = Sets.newHashSet();
        Map<String, List<Event>> eventsMap = Maps.newHashMap();
        populateCollections(timbermillEvents, nodesMap, startEventsIds, parentIds, eventsMap);

        Set<String> missingParentsIds = parentIds.stream().filter(id -> !startEventsIds.contains(id)).collect(Collectors.toSet());
        Map<String, Task> previouslyIndexedParentTasks = getMissingParents(missingParentsIds, env);
        connectNodesByParentId(nodesMap);

        Map<String, Task> tasksMap = createEnrichedTasks(nodesMap, eventsMap, previouslyIndexedParentTasks);
        resolveOrphansFromCache(tasksMap);

        String index = es.createTimbermillAlias(env);
        Map<String, String> idToIndex = es.index(tasksMap, index);
        updateIndexToTasks(tasksMap, idToIndex);
        cacheTasks(tasksMap);
        cacheOrphans(tasksMap);

        if (!index.endsWith(ElasticsearchUtil.getIndexSerial(1))){
            es.rolloverIndex(index);
        }
        LOG.info("{} tasks were indexed to elasticsearch", tasksMap.size());
        return previouslyIndexedParentTasks.size();
    }

    private void updateIndexToTasks(Map<String, Task> tasksMap, Map<String, String> idToIndex) {
        for (Map.Entry<String, String> entry : idToIndex.entrySet()) {
            String id = entry.getKey();
            String index = entry.getValue();
            Task task = tasksMap.get(id);
            if (task != null){
                task.setIndex(index);
            }
        }
    }

    private void resolveOrphansFromCache(Map<String, Task> tasksMap) {
        Timer.Started start = KamonConstants.ORPHANS_JOB_LATENCY.withoutTags().start();

        ParentResolver resolver = new ParentResolver(tasksMap, cacheHandler);
        Map<String, Task> adoptedTasksMap = resolver.resolveOrphansReceived();

        for (Map.Entry<String, Task> adoptedEntry : adoptedTasksMap.entrySet()) {
            String adoptedId = adoptedEntry.getKey();
            Task adoptedTask = adoptedEntry.getValue();
            if (tasksMap.containsKey(adoptedId)){
                tasksMap.get(adoptedId).mergeTask(adoptedTask, adoptedId);
            }
            else{
                tasksMap.put(adoptedId, adoptedTask);
            }
        }
        int adopted = adoptedTasksMap.size();
        if (adopted > 0) {
            LOG.info("{} orphans resolved", adopted);
        }
        KamonConstants.ORPHANS_ADOPTED_HISTOGRAM.withoutTags().record(adopted);
        start.stop();
    }

    private void cacheOrphans(Map<String, Task> tasksMap) {
        Map<String, List<String>> parentToOrphansMap = Maps.newHashMap();

        for (Map.Entry<String, Task> entry : tasksMap.entrySet()) {
            Task orphanTask = entry.getValue();
            String orphanId = entry.getKey();
            String parentId = orphanTask.getParentId();
            if (parentId != null) {
                if (orphanTask.isOrphan() != null && orphanTask.isOrphan()) {
                    List<String> tasks = parentToOrphansMap.get(parentId);
                    if (tasks == null) {
                        tasks = Lists.newArrayList(orphanId);
                    } else {
                        tasks.add(orphanId);
                    }
                    parentToOrphansMap.put(parentId, tasks);
                }
            }
        }

        if (!parentToOrphansMap.isEmpty()) {
            Map<String, List<String>> fromOrphansCache = cacheHandler.logPullFromOrphansCache(parentToOrphansMap.keySet(), "cache_orphans");
            for (Map.Entry<String, List<String>> entry : fromOrphansCache.entrySet()) {
                String parentId = entry.getKey();
                List<String> orphansList = parentToOrphansMap.get(parentId);
                List<String> orphanListFromCache = entry.getValue();
                orphansList.addAll(orphanListFromCache);
            }

            cacheHandler.logPushToOrphanCache(parentToOrphansMap, "cache_orphans");
        }
    }

    private void cacheTasks(Map<String, Task> tasksMap) {
        HashMap<String, LocalTask> updatedTasks = Maps.newHashMap();
        Map<String, LocalTask> idToTaskMap = cacheHandler.logGetFromTasksCache(tasksMap.keySet(), "cache_tasks");
        for (Map.Entry<String, Task> entry : tasksMap.entrySet()) {
            Task task = entry.getValue();
            LocalTask localTask = new LocalTask(task);
            String id = entry.getKey();
            Task cachedTask = idToTaskMap.get(id);
            if (cachedTask != null) {
                localTask.mergeTask(cachedTask, id);
            }
            updatedTasks.put(id, localTask);
        }
        cacheHandler.logPushToTasksCache(updatedTasks, "cache_tasks");
    }

    private Map<String, Task> getMissingParents(Set<String> parentIds, String env) {
        
        int missingParentAmount = parentIds.size();
        KamonConstants.MISSING_PARENTS_HISTOGRAM.withoutTags().record(missingParentAmount);
        LOG.info("Fetching {} missing parents", missingParentAmount);

        Map<String, Task> previouslyIndexedParentTasks = Maps.newHashMap();
        try {
            if (!parentIds.isEmpty()) {
                Map<String, LocalTask> parentMap = cacheHandler.logGetFromTasksCache(parentIds, "missing_parents");
                parentMap.forEach((parentId, parentTask) -> {
                    if (parentTask != null) {
                        previouslyIndexedParentTasks.put(parentId, parentTask);
                    }
                });
            }
        } catch (Throwable t) {
            LOG.error("Error fetching indexed tasks from Elasticsearch", t);
        }

        parentIds.removeAll(previouslyIndexedParentTasks.keySet());
        if (!parentIds.isEmpty()) {
            Map<String, Task> fromEs = es.getMissingParents(parentIds, env);
            previouslyIndexedParentTasks.putAll(fromEs);

            LOG.info("Fetched {} missing parents from Elasticsearch", fromEs.size());
        }

        return previouslyIndexedParentTasks;
    }

    private void reportBatchMetrics(String env, int tasksFetchedSize, ZonedDateTime taskIndexerStartTime, int indexedTasksSize) {
        ZonedDateTime taskIndexerEndTime = ZonedDateTime.now();
        long timesDuration = ElasticsearchUtil.getTimesDuration(taskIndexerStartTime, taskIndexerEndTime);
        reportToElasticsearch(env, tasksFetchedSize, taskIndexerStartTime, indexedTasksSize, timesDuration, taskIndexerEndTime);
        reportToKamon(tasksFetchedSize, indexedTasksSize);
    }

    private void reportToKamon(int tasksFetchedSize, int indexedTasksSize) {
        KamonConstants.MISSING_PARENTS_TASKS_FETCHED_HISTOGRAM.withoutTags().record(tasksFetchedSize);
        KamonConstants.TASKS_INDEXED_HISTOGRAM.withoutTags().record(indexedTasksSize);
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

    private boolean isOrphan(Event event, Map<String, Task> previouslyIndexedTasks, Map<String, List<Event>> eventsMap) {
        String parentId = event.getParentId();
        if (parentId == null) {
            return false;
        } else {
            if (previouslyIndexedTasks.containsKey(parentId)){
                Task parentTask = previouslyIndexedTasks.get(parentId);
                return parentTask.isOrphan() != null && parentTask.isOrphan();
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
}

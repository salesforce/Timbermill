package com.datorama.oss.timbermill;

import com.datorama.oss.timbermill.common.Constants;
import com.datorama.oss.timbermill.common.ElasticsearchUtil;
import com.datorama.oss.timbermill.common.KamonConstants;
import com.datorama.oss.timbermill.plugins.PluginsConfig;
import com.datorama.oss.timbermill.plugins.TaskLogPlugin;
import com.datorama.oss.timbermill.unit.*;
import com.google.common.cache.*;
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

public class TaskIndexer {

    private static final Logger LOG = LoggerFactory.getLogger(TaskIndexer.class);

    private final ElasticsearchClient es;
    private final Collection<TaskLogPlugin> logPlugins;
    private Cache<String, LocalTask> tasksCache;
    private Cache<String, List<String>> orphansCache;
    private long daysRotation;
    private String timbermillVersion;
    private int recursionMax;

    public TaskIndexer(String pluginsJson, Integer daysRotation, ElasticsearchClient es, String timbermillVersion, int maximumCacheWeight, int recursionMax) {

        this.recursionMax = recursionMax;
        this.daysRotation = calculateDaysRotation(daysRotation);
        this.logPlugins = PluginsConfig.initPluginsFromJson(pluginsJson);
        this.es = es;
        this.timbermillVersion = timbermillVersion;
        tasksCache = CacheBuilder.newBuilder()
                .maximumWeight(maximumCacheWeight)
                .weigher((Weigher<String, LocalTask>) (key, value) -> key.length() + value.estimatedSize())
                .removalListener(notification -> {
                    String key = notification.getKey();
                    LocalTask value = notification.getValue();
                    KamonConstants.TASKS_IN_TASK_CACHE.withoutTags().decrement(key.length() + value.estimatedSize());
                })
                .build();
        orphansCache = CacheBuilder.newBuilder()
                .maximumWeight(maximumCacheWeight)
                .weigher((Weigher<String, List<String>>) (key, value) -> key.length() + value.stream().mapToInt(String::length).sum())
                .removalListener(notification -> {
                    int length = getEntryLength(notification.getKey(), notification.getValue());
                    KamonConstants.ORPHANS_CACHE_RANGE_SAMPLER.withoutTags().decrement(length);
                })
                .build();
    }

    private int getEntryLength(String key, List<String> value) {
        int valuesLengths = value.stream().mapToInt(String::length).sum();
        int keyLegth = key.length();
        return keyLegth + valuesLengths;
    }

    private static int calculateDaysRotation(int daysRotationParam) {
        return Math.max(daysRotationParam, 1);
    }

    public void close() {
        es.close();
    }

    public void retrieveAndIndex(Collection<Event> events, String env) {
        String flowId = "Task Indexer - " + UUID.randomUUID().toString();
        ThreadContext.put("id", flowId);
        LOG.info("#### Batch Start ####");
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
            handleTimbermillEvents(env, taskIndexerStartTime, timbermillEvents);
        }
        LOG.info("#### Batch End ####");
    }

    private void handleTimbermillEvents(String env, ZonedDateTime taskIndexerStartTime, Collection<Event> timbermillEvents) {
        applyPlugins(timbermillEvents, env);

        Map<String, DefaultMutableTreeNode> nodesMap = Maps.newHashMap();
        Set<String> startEventsIds = Sets.newHashSet();
        Set<String> parentIds = Sets.newHashSet();
        Map<String, List<Event>> eventsMap = Maps.newHashMap();
        populateCollections(timbermillEvents, nodesMap, startEventsIds, parentIds, eventsMap);

        Set<String> missingParentsIds = parentIds.stream().filter(id -> !startEventsIds.contains(id)).collect(Collectors.toSet());
        Map<String, Task> previouslyIndexedParentTasks = getMissingParents(missingParentsIds);
        connectNodesByParentId(nodesMap);

        Map<String, Task> tasksMap = createEnrichedTasks(nodesMap, eventsMap, previouslyIndexedParentTasks);
        cacheTasks(tasksMap);
        cacheOrphans(tasksMap);

        resolveOrphansFromCache(tasksMap);


        String index = es.createTimbermillAlias(env);
        Map<String, String> idToIndex = es.index(tasksMap, index);
        updateIndexToTasks(idToIndex);

        if (!index.endsWith(ElasticsearchUtil.getIndexSerial(1))){
            es.rolloverIndex(index);
        }
        LOG.info("{} tasks were indexed to elasticsearch", tasksMap.size());
        reportBatchMetrics(env, previouslyIndexedParentTasks.size(), taskIndexerStartTime, timbermillEvents.size());
    }

    private void updateIndexToTasks(Map<String, String> idToIndex) {
        for (Map.Entry<String, String> entry : idToIndex.entrySet()) {
            String id = entry.getKey();
            String index = entry.getValue();
            LocalTask localTask = tasksCache.getIfPresent(id);
            if (localTask != null){
                localTask.setIndex(index);
            }
        }
    }

    private void resolveOrphansFromCache(Map<String, Task> tasksMap) {
        Timer.Started start = KamonConstants.ORPHANS_JOB_LATENCY.withoutTags().start();
        Map<String, LocalTask> adoptedTasksMap = Maps.newHashMap();
        for (Map.Entry<String, Task> entry : tasksMap.entrySet()) {
            String id = entry.getKey();
            Task parentTask = entry.getValue();
            if (parentTask.getStatus() == TaskStatus.UNTERMINATED || parentTask.getStatus() == TaskStatus.SUCCESS || parentTask.getStatus() == TaskStatus.ERROR) {
                resolveOrphan(id, parentTask, adoptedTasksMap, 1);
            }
        }

        for (Map.Entry<String, LocalTask> adoptedEntry : adoptedTasksMap.entrySet()) {
            String adoptedId = adoptedEntry.getKey();
            LocalTask adoptedTask = adoptedEntry.getValue();
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

        KamonConstants.ORPHANS_FOUND_HISTOGRAM.withoutTags().record(orphansCache.size());
        KamonConstants.ORPHANS_ADOPTED_HISTOGRAM.withoutTags().record(adopted);
        start.stop();
    }

    private void resolveOrphan(String parentId, Task parentIndexedTask, Map<String, LocalTask> adoptedTasksMap, int counter) {
        if (counter > this.recursionMax){
            return;
        }
        if (parentIndexedTask.isOrphan() == null || !parentIndexedTask.isOrphan()){
            List<String> orphans = orphansCache.getIfPresent(parentId);
            if (orphans != null) {
                orphansCache.invalidate(parentId);
                for (String orphanId : orphans) {
                    LocalTask adoptedTask = tasksCache.getIfPresent(orphanId);
                    if (adoptedTask == null){
                        LOG.warn("Missing task from local cache {}", orphanId);
                    }
                    else {
                        adoptedTask.setOrphan(false);

                        populateParentParams(adoptedTask, parentIndexedTask);
                        adoptedTasksMap.put(orphanId, adoptedTask);
                        resolveOrphan(orphanId, adoptedTask, adoptedTasksMap, counter + 1);
                    }
                }
            }
        }
    }

    private void cacheOrphans(Map<String, Task> tasksMap) {
        for (Map.Entry<String, Task> entry : tasksMap.entrySet()) {
            Task orphanTask = entry.getValue();
            String orphanId = entry.getKey();
            String parentId = orphanTask.getParentId();
            if (parentId != null) {
                if (orphanTask.isOrphan() != null && orphanTask.isOrphan()) {
                    List<String> tasks = orphansCache.getIfPresent(parentId);
                    if (tasks == null) {
                        tasks = Lists.newArrayList(orphanId);
                    } else {
                        tasks.add(orphanId);
                    }
                    orphansCache.put(parentId, tasks);
                    int entryLength = getEntryLength(parentId, tasks);
                    KamonConstants.ORPHANS_CACHE_RANGE_SAMPLER.withoutTags().increment(entryLength);
                }
            }
        }
    }

    private void cacheTasks(Map<String, Task> tasksMap) {
        for (Map.Entry<String, Task> entry : tasksMap.entrySet()) {
            Task task = entry.getValue();
            LocalTask localTask = new LocalTask(task);
            String id = entry.getKey();
            LocalTask cachedTask = tasksCache.getIfPresent(id);
            if (cachedTask != null) {
                localTask.mergeTask(cachedTask, id);
            }
            KamonConstants.TASKS_IN_TASK_CACHE.withoutTags().increment(id.length() + localTask.estimatedSize());
            tasksCache.put(id, localTask);
        }
    }

    private Map<String, Task> getMissingParents(Set<String> parentIds) {
        
        int missingParentAmount = parentIds.size();
        KamonConstants.MISSING_PARENTS_HISTOGRAM.withoutTags().record(missingParentAmount);
        LOG.info("Fetching {} missing parents", missingParentAmount);
        
        Map<String, Task> previouslyIndexedParentTasks = Maps.newHashMap();
        try {
            if (!parentIds.isEmpty()) {
                parentIds.forEach(parentId -> {
                    Task parentTask = tasksCache.getIfPresent(parentId);
                    if (parentTask != null) {
                        previouslyIndexedParentTasks.put(parentId, parentTask);
                    }
                });
            }
        } catch (Throwable t) {
            LOG.error("Error fetching indexed tasks from Elasticsearch", t);
        }
        LOG.info("Fetched {} missing parents", previouslyIndexedParentTasks.size());
        return previouslyIndexedParentTasks;
    }

    private void reportBatchMetrics(String env, int tasksFetchedSize, ZonedDateTime taskIndexerStartTime, int indexedTasksSize) {
        ZonedDateTime now = ZonedDateTime.now();
        long timesDuration = ElasticsearchUtil.getTimesDuration(taskIndexerStartTime, now);
        reportToElasticsearch(env, tasksFetchedSize, taskIndexerStartTime, indexedTasksSize, timesDuration, now);
        reportToKamon(tasksFetchedSize, indexedTasksSize, timesDuration);
    }

    private void reportToKamon(int tasksFetchedSize, int indexedTasksSize, long duration) {
        KamonConstants.MISSING_PARENTS_TASKS_FETCHED_HISTOGRAM.withoutTags().record(tasksFetchedSize);
        KamonConstants.TASKS_INDEXED_HISTOGRAM.withoutTags().record(indexedTasksSize);
        KamonConstants.BATCH_DURATION_HISTOGRAM.withoutTags().record(duration);
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

    private static void populateParentParams(Event event, Task parentIndexedTask, Collection<Event> parentCurrentEvent) {
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

    private static void populateParentParams(Task task, Task parentIndexedTask) {
        ParentProperties parentProperties = getParentProperties(parentIndexedTask, null);
        List<String> parentsPath =  new ArrayList<>();
        String primaryId = parentProperties.getPrimaryId();
        task.setPrimaryId(primaryId);
        if (task.getCtx() == null){
            task.setCtx(Maps.newHashMap());
        }
        for (Map.Entry<String, String> entry : parentProperties.getContext().entrySet()) {
            task.getCtx().putIfAbsent(entry.getKey(), entry.getValue());
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
            task.setParentsPath(parentsPath);
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

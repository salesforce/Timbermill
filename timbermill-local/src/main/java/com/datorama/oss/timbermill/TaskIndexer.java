package com.datorama.oss.timbermill;

import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
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
import com.google.common.cache.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import static com.datorama.oss.timbermill.ElasticsearchClient.CTX_FIELDS;
import static com.datorama.oss.timbermill.common.Constants.GSON;
import static java.util.stream.Collectors.groupingBy;
import static org.elasticsearch.common.Strings.EMPTY_ARRAY;

public class TaskIndexer {

    private static final Logger LOG = LoggerFactory.getLogger(TaskIndexer.class);
    private static final String TEXT = "text";
    private static final String STRING = "string";

    private static final String CTX = "ctx";
    private final ElasticsearchClient es;
    private final Collection<TaskLogPlugin> logPlugins;
    private final Cache<String, Queue<AdoptedEvent>> parentIdTORootOrphansEventsCache;
    private long daysRotation;
    private String missingParentTask;

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
        trimAllStrings(events);

        Collection<Event> heartbeatEvents = new HashSet<>();
        Collection<Event> timbermillEvents = new LinkedHashSet<>();

        events.forEach(e -> {
            if (missingParentTask != null && e.getTaskId().equals(missingParentTask)){
                LOG.info("FOUND MISSING PARENT {}", missingParentTask);
                missingParentTask = null;
            }



            if (e.getName() != null && e.getName().equals(Constants.HEARTBEAT_TASK)){
                heartbeatEvents.add(e);
            }
            else{
                timbermillEvents.add(e);
            }
        });

        if (!heartbeatEvents.isEmpty()) {
            indexHeartbeatEvents(env, heartbeatEvents);
        }

        LOG.info("{} events to handle in current batch", timbermillEvents.size());

        if (!timbermillEvents.isEmpty()) {
            IndexEvent indexEvent = handleTimbermillEvents(env, taskIndexerStartTime, timbermillEvents);
            es.indexMetaDataTasks(env, GSON.toJson(indexEvent));
        }
        LOG.info("------------------ Batch End ------------------");
    }

    private IndexEvent handleTimbermillEvents(String env, ZonedDateTime taskIndexerStartTime, Collection<Event> timbermillEvents) {
        Map<String, Task> previouslyIndexedParentTasks = fetchPreviouslyIndexedParentTasks(timbermillEvents);
        applyPlugins(timbermillEvents, env);
        Map<String, Task> tasksMap = createEnrichedTasks(timbermillEvents, previouslyIndexedParentTasks);
        String index = es.createTimbermillAlias(env);
        es.index(tasksMap, index);
        es.rolloverIndex(index);
        LOG.info("{} tasks were indexed to elasticsearch", timbermillEvents.size());
        return new IndexEvent(env, previouslyIndexedParentTasks.size(), taskIndexerStartTime, ZonedDateTime.now(), timbermillEvents.size(), daysRotation);
    }

    private Map<String, Task> fetchPreviouslyIndexedParentTasks(Collection<Event> timbermillEvents) {
        Set<String> missingParentTaskIds = getMissingParentTaskIds(timbermillEvents);
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

    private void updateAdoptedOrphans(Map<String, List<Event>> eventsMap, String parentTaskId) {
        Queue<AdoptedEvent> adoptedEvents = parentIdTORootOrphansEventsCache.getIfPresent(parentTaskId);

        if (adoptedEvents != null) {
            parentIdTORootOrphansEventsCache.invalidate(parentTaskId);
            populateWithContextValue(adoptedEvents);
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

    private void populateWithContextValue(Queue<AdoptedEvent> adoptedEvent) {
        Set<String> taskIds = adoptedEvent.stream().map(Event::getTaskId).collect(Collectors.toSet());
        Map<String, Task> tasks = this.es.getTasksByIds(null, taskIds, "Get adopted tasks context", CTX_FIELDS, EMPTY_ARRAY);

        adoptedEvent.forEach(e -> {
            if (e.getContext() == null){
                e.setContext(Maps.newHashMap());
            }
            Task task = tasks.get(e.getTaskId());
            if (task != null){
                for (Map.Entry<String, String> entry : task.getCtx().entrySet()) {
                    e.getContext().putIfAbsent(entry.getKey(), entry.getValue());
                }
            }
        });
    }

    private void enrichEvent(Map<String, List<Event>> eventsMap, Map<String, Task> previouslyIndexedTasks, Event event) {
        String parentId = event.getParentId();
        if (parentId != null) {
            if (isOrphan(event, previouslyIndexedTasks, eventsMap)){
                event.setOrphan(true);
                event.setPrimaryId(null);
                addOrphanToCache(event, parentId);
            }
            else {
                populateParentParams(event, previouslyIndexedTasks.get(parentId), eventsMap.get(parentId));
            }
        }
        else{
            event.setPrimaryId(event.getTaskId());
        }
        if (hasAdoptedOrphans(event)) {
            updateAdoptedOrphans(eventsMap, event.getTaskId());
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

    private void addOrphanToCache(Event event, String parentId) {
        Queue<AdoptedEvent> eventList = parentIdTORootOrphansEventsCache.getIfPresent(parentId);

        AdoptedEvent orphanEvent = new AdoptedEvent(event);
        if (eventList == null) {
            eventList = new LinkedList<>();
            eventList.add(orphanEvent);
            parentIdTORootOrphansEventsCache.put(parentId, eventList);

            if (missingParentTask == null){
                LOG.info("PARENT TASK {}", parentId);
                missingParentTask = parentId;
            }



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
                        LOG.warn("Problem with parent events. Evens: {}", parentEvents.toString());
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
                long duration = ElasticsearchUtil.getTimesDuration(startTime, endTime);
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
						LOG.warn("NaN value for key {} in ID {}. Changed to 0", key, event.getTaskId());
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
            value = trimValue(type, key, value, event, Constants.MAX_CHARS_ALLOWED_FOR_ANALYZED_FIELDS);
        } else {
            value = trimValue(type, key, value, event, Constants.MAX_CHARS_ALLOWED_FOR_NON_ANALYZED_FIELDS);
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

    private static Set<String> getMissingParentTaskIds(Collection<Event> timbermillEvents) {
        Set<String> startEventsIds = timbermillEvents.stream().filter(Event::isStartEvent).map(Event::getTaskId).collect(Collectors.toSet());
        return timbermillEvents.stream()
                .filter(e -> e.getParentId() != null && !startEventsIds.contains(e.getParentId()))
                .map(Event::getParentId).collect(Collectors.toSet());
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
}

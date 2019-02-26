package com.datorama.timbermill.pipe;

import com.datorama.timbermill.*;
import com.datorama.timbermill.Task.TaskStatus;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.elasticsearch.ElasticsearchException;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

public class LocalTaskIndexer {

    private final ElasticsearchClient es;
    private BlockingQueue<Event> eventsQueue = new ArrayBlockingQueue<>(1000000);
    private static final Logger LOG = LoggerFactory.getLogger(LocalTaskIndexer.class);
    private static final String INCOMING_EVENTS = "incomingEvents";
    private static final String MISSING_START_EVENTS = "missingStartEvents";
    private static final String MISSING_PARENTS_EVENTS = "missingParentEvents";
    private static final String PREV_INDEXED_TASKS_FETCHED = "prevIndexedTasksFetched";
    private static final String INDEXED_TASKS = "indexedTasks";
    private Collection<TaskLogPlugin> logPlugins;
    private String profile;
    private Map<String, Integer> propertiesLengthMap;
    private int defaultMaxChars;

    public LocalTaskIndexer(String pluginsJson, String profile, Map<String, Integer> propertiesLengthMap, int defaultMaxChars, ElasticsearchClient es) {
        this.profile = profile;
        logPlugins = PluginsConfig.initPluginsFromJson(pluginsJson);
        this.propertiesLengthMap = propertiesLengthMap;
        this.defaultMaxChars = defaultMaxChars;
        this.es = es;
        new Thread(() -> {
            while(true){
                retrieveAndIndex();
            }
        }).start();
    }

    public void retrieveAndIndex() {
        LOG.info("------------------ Batch Start ------------------");
        List<Event> events = new ArrayList<>();
        eventsQueue.drainTo(events, 100);
        DateTime taskIndexerStartTime = new DateTime();
        trimAttributesAndData(events);
        List<Event> heartbeatEvents = events.stream().filter(e -> (e.getTaskType() != null) && e.getTaskType().equals(Constants.HEARTBEAT_TASK)).collect(toList());
        for (Event e: heartbeatEvents){
            Task heartbeatTask = new Task(e.getTaskId());
            heartbeatTask.update(e);
            this.es.indexTaskToMetaDataIndex(heartbeatTask);
        }

        List<Event> timbermillEvents = events.stream().filter(e -> (e.getTaskType() == null) || !e.getTaskType().equals(Constants.HEARTBEAT_TASK)).collect(toList());

        LOG.info("{} events retrieved from pipe", timbermillEvents.size());
        Set<String> missingStartEvents = getMissingStartEvents(timbermillEvents);
        Set<String> missingParentEvents = getMissingParentEvents(timbermillEvents);

        if (!missingStartEvents.isEmpty()){
            LOG.info("{} missing start events for current batch", missingStartEvents.size());
        }
        if (!missingParentEvents.isEmpty()) {
            LOG.info("{} missing parent events for current batch", missingParentEvents.size());
        }

        Set<String> missingEvents = new LinkedHashSet<>();
        missingEvents.addAll(missingStartEvents);
        missingEvents.addAll(missingParentEvents);
        Map<String, Task> previouslyIndexedTasks = new HashMap<>();
        Map<String, Task> tasksToIndex = new HashMap<>();
        try {
            previouslyIndexedTasks = this.es.fetchIndexedTasks(missingEvents);

            if (!missingEvents.isEmpty()) {
                if (missingEvents.size() == previouslyIndexedTasks.size()) {
                    LOG.info("All {} missing event were retreived from elasticsearch..", missingEvents.size());
                } else {
                    LOG.warn("{} missing events were not retrieved from elasticsearch", missingEvents.size() - previouslyIndexedTasks.size(),
                            StringUtils.join(missingEvents.removeAll(previouslyIndexedTasks.keySet())), ", ");
                }
            }

            tasksToIndex = prepareTasksToIndex(timbermillEvents, previouslyIndexedTasks);

            long pluginsDuration = applyPlugins(timbermillEvents, tasksToIndex);

            this.es.indexTasks(tasksToIndex);
            if (!tasksToIndex.isEmpty()) {
                LOG.info("{} tasks were indexed to elasticsearch", tasksToIndex.size());
                indexMetadataTask(taskIndexerStartTime, timbermillEvents.size(), missingStartEvents.size(), missingParentEvents.size(), previouslyIndexedTasks.size(), tasksToIndex.size(), pluginsDuration);
            }
            LOG.info("------------------ Batch End ------------------");
        } catch (ElasticsearchException e){
            indexFailedMetadataTask(taskIndexerStartTime, timbermillEvents.size(), missingStartEvents.size(), missingParentEvents.size(), previouslyIndexedTasks.size(), tasksToIndex.size(), e);
        }
    }

    public void addEvent(Event e) {
        eventsQueue.add(e);
    }

    private long applyPlugins(List<Event> events, Map<String, Task> tasks) {
        long pluginsStart = System.currentTimeMillis();
        try {
            String taskType = "timbermill_plugin";
            List<Task> metaTasks = new ArrayList<>();
            for (TaskLogPlugin plugin : logPlugins) {
                Task t = new Task(taskType + '_' + plugin.toString().replace(' ', '_') + "_" + pluginsStart);
                try {
                    t.setTaskType(taskType);
                    t.setPrimary(true);
                    t.setStartTime(new DateTime());
                    t.getAttributes().put("pluginName", plugin.getName());
                    t.getAttributes().put("pluginClass", plugin.getClass().getSimpleName());

                    plugin.apply(events, tasks);

                    t.setEndTime(new DateTime());
                    t.setTotalDuration(t.getEndTime().getMillis() - t.getStartTime().getMillis());
                    t.setStatus(TaskStatus.SUCCESS);
                } catch (Exception ex) {
                    t.getData().put("error", ex.toString());
                    t.setStatus(TaskStatus.ERROR);
                    LOG.error("error in plugin" + plugin, ex);
                }
                metaTasks.add(t);
                es.indexTaskToMetaDataIndex(t);
            }
        } catch (Exception ex) {
            LOG.error("Error running plugins", ex);
        }
        long pluginsEnd = System.currentTimeMillis();
        return pluginsEnd - pluginsStart;
    }

    private String getProfile() {
        return profile;
    }

    private void trimAttributesAndData(List<Event> events) {
        events.forEach(e -> {
            e.setAttributes(getTrimmedLongValues(e.getAttributes(), "attributes"));
            e.setData(getTrimmedLongValues(e.getData(), "data"));
        });
    }

    private Map<String, String> getTrimmedLongValues(Map<String, ?> oldMap, String prefix) {
        Map<String, String> newMap = new HashMap<>();
        for (Map.Entry<String, ?> entry : oldMap.entrySet()){
            String key = entry.getKey();
            String value = String.valueOf(entry.getValue());
            value = trimIfNeededValue(prefix + "." + key, value);
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

    private void indexFailedMetadataTask(DateTime taskIndexerStartTime, Integer timbermillEventSize, Integer missingStartEventsSize, Integer missingParentEventsSize,
                                         Integer previouslyIndexedTasksSize, Integer tasksToIndexSize, Exception e) {
        Task metadataTask = createMetadataTask(taskIndexerStartTime, timbermillEventSize, missingStartEventsSize, missingParentEventsSize, previouslyIndexedTasksSize, tasksToIndexSize);
        metadataTask.setStatus(TaskStatus.ERROR);
        Map<String, String> exceptionMap = new HashMap<>();
        exceptionMap.put("exception", ExceptionUtils.getStackTrace(e));
        metadataTask.setData(exceptionMap);
        es.indexTaskToMetaDataIndex(metadataTask);
    }

    private void indexMetadataTask(DateTime taskIndexerStartTime, Integer timbermillEventSize, Integer missingStartEventsSize, Integer missingParentEventsSize,
                                   Integer previouslyIndexedTasksSize, Integer tasksToIndexSize, long pluginsDuration) {

        Task metadataTask = createMetadataTask(taskIndexerStartTime, timbermillEventSize, missingStartEventsSize, missingParentEventsSize, previouslyIndexedTasksSize, tasksToIndexSize);
        metadataTask.setStatus(TaskStatus.SUCCESS);
        metadataTask.getMetrics().put("pluginsDuration", pluginsDuration);
        es.indexTaskToMetaDataIndex(metadataTask);
    }

    private static Task createMetadataTask(DateTime taskIndexerStartTime, Integer timbermillEventSize, Integer missingStartEventsSize, Integer missingParentEventsSize,
                                           Integer previouslyIndexedTasksSize,
                                           Integer tasksToIndexSize) {
        DateTime taskIndexerEndTime = new DateTime();

        String timbermillIndexType = "timbermill_index";
        Task metadataTask = new Task(timbermillIndexType + '-' + taskIndexerStartTime.getMillis());
        metadataTask.setTaskType(timbermillIndexType);
        metadataTask.setPrimary(true);
        Map<String, Number> taskIndexerMetrics = new HashMap<>();

        taskIndexerMetrics.put(INCOMING_EVENTS, timbermillEventSize);
        taskIndexerMetrics.put(MISSING_START_EVENTS, missingStartEventsSize);
        taskIndexerMetrics.put(MISSING_PARENTS_EVENTS, missingParentEventsSize);
        taskIndexerMetrics.put(PREV_INDEXED_TASKS_FETCHED, previouslyIndexedTasksSize);
        taskIndexerMetrics.put(INDEXED_TASKS, tasksToIndexSize);

        metadataTask.setMetrics(taskIndexerMetrics);

        metadataTask.setStartTime(taskIndexerStartTime);
        metadataTask.setEndTime(taskIndexerEndTime);
        metadataTask.setTotalDuration(taskIndexerEndTime.getMillis() - taskIndexerStartTime.getMillis());
        return metadataTask;
    }

    private static Set<String> getMissingStartEvents(Collection<Event> events) {
        Set<String> startEventTasks = events.stream()
                .filter(e -> (e.getEventType() == EventType.START) || (e.getEventType() == EventType.SPOT))
                .map(e -> e.getTaskId()).collect(Collectors.toCollection(LinkedHashSet::new));

        return Sets.difference(getAllTaskIds(events), startEventTasks);
    }

    private static Set<String> getMissingParentEvents(Collection<Event> events) {
        Set<String> allEventParentTasks = events.stream()
                .filter(e -> e.getParentTaskId() != null)
                .map(e -> e.getParentTaskId()).collect(Collectors.toCollection(LinkedHashSet::new));

        return Sets.difference(allEventParentTasks, getAllTaskIds(events));
    }

    private static Iterable<String> getStartEventsTaskIds(Collection<Event> events) {
        return events.stream()
                .filter(e -> (e.getEventType() == EventType.START) || (e.getEventType() == EventType.SPOT))
                .map(e -> e.getTaskId()).collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static Iterable<String> getEndEventsTaskIds(Collection<Event> events) {
        return events.stream()
                .filter(e -> (e.getEventType() == EventType.END_SUCCESS)
                        || (e.getEventType() == EventType.END_APP_ERROR)
                        || (e.getEventType() == EventType.END_ERROR)
                        || (e.getEventType() == EventType.SPOT))
                .map(e -> e.getTaskId()).collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static Set<String> getAllTaskIds(Collection<Event> events) {
        return events.stream().map(e -> e.getTaskId()).collect(Collectors.toSet());
    }

    private Map<String, Task> prepareTasksToIndex(Collection<Event> events, Map<String, Task> previouslyIndexedTasks) {

        Map<String, List<Event>> groupedEvents = events.stream().collect(groupingBy(e -> e.getTaskId()));

        Map<String, Task> tasksToIndex = new HashMap<>();

        for (Map.Entry<String, List<Event>> idToEventEntry : groupedEvents.entrySet()) {
            Task t;
            if (previouslyIndexedTasks.containsKey(idToEventEntry.getKey())) {
                t = previouslyIndexedTasks.get(idToEventEntry.getKey());
            }
            else if (tasksToIndex.containsKey(idToEventEntry.getKey())){
                t = tasksToIndex.get(idToEventEntry.getKey());
            }
            else {
                t = new Task(idToEventEntry.getKey());
            }

            t.update(idToEventEntry.getValue());

            //Start event was not indexed
            if (t.taskWithNoStartEvent()){
                LOG.error("Task id {} start event is missing from the elasticsearch", t.getTaskId());
                t.setStatus(TaskStatus.CORRUPTED);
                t.setStartTime(t.getEndTime());
            }
            t.setEnv(getProfile());
            tasksToIndex.put(idToEventEntry.getKey(), t);
        }

        /*
         * Compute origins and down merge parameters from parent
         */
        for (String tid : getStartEventsTaskIds(events)) {
            Task t = tasksToIndex.get(tid);
            List<String> origins = new ArrayList<>();
            String parentTaskId = t.getParentTaskId();
            if (parentTaskId != null) {
                Task parentTask = getParentTask(previouslyIndexedTasks, tasksToIndex, parentTaskId);
                if (parentTask != null) {
                    t.setPrimaryTaskId(parentTask.getPrimaryTaskId());
                    for (Map.Entry<String, Object> entry : parentTask.getAttributes().entrySet()) {
                        t.getAttributes().putIfAbsent(entry.getKey(), entry.getValue());
                    }
                    if((parentTask.getOrigins() != null) && !parentTask.getOrigins().isEmpty()) {
                        origins.addAll(parentTask.getOrigins());
                    }
                    origins.add(parentTask.getTaskType());
                }
                else {
                    t.setPrimaryTaskId(t.getTaskId());
                    t.setParentTaskId(null);
                }
            }
            if(!origins.isEmpty()) {
                t.setOrigins(origins);
                t.setPrimaryOrigin(origins.get(0));
            }
        }

        for (String tid : getEndEventsTaskIds(events)){
            Task t = tasksToIndex.get(tid);
            String parentTaskId = t.getParentTaskId();
            long totalDuration = t.getEndTime().getMillis() - t.getStartTime().getMillis();
            t.setTotalDuration(totalDuration);
            t.setSelfDuration(Math.max(0, totalDuration - t.getChildrenDelta()));
            if (parentTaskId != null) {
                Task parentTask = getParentTask(previouslyIndexedTasks, tasksToIndex, parentTaskId);
                if( parentTask != null) {
                    parentTask.updateChildrenDelta(totalDuration);
                    if (!tasksToIndex.containsKey(parentTaskId)) {
                        tasksToIndex.put(parentTaskId, parentTask);
                    }
                }
                else{
                    LOG.error("Parent task with id {} for child task with id {} could not be found in elasticsearch", parentTaskId, t.getTaskId());
                }
            }
        }

        return tasksToIndex;
    }

    private static Task getParentTask(Map<String, Task> previouslyIndexedTasks, Map<String, Task> tasksToIndex, String parentTaskId) {
        Task parentTask = tasksToIndex.get(parentTaskId);
        if (parentTask == null) {
            parentTask = previouslyIndexedTasks.get(parentTaskId);
        }
        return parentTask;
    }
}

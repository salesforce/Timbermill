package com.datorama.timbermill;


import com.datorama.timbermill.common.Constants;
import com.datorama.timbermill.plugins.PluginsConfig;
import com.datorama.timbermill.plugins.TaskLogPlugin;
import com.datorama.timbermill.unit.Event;
import com.datorama.timbermill.unit.Task;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.ElasticsearchException;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

import static com.datorama.timbermill.unit.Event.*;
import static com.datorama.timbermill.unit.Task.*;
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
    private Map<String, Integer> propertiesLengthMap;
    private int defaultMaxChars;
    private boolean keepRunning = true;
    private boolean stoppedRunning = false;

    public LocalTaskIndexer(String pluginsJson, Map<String, Integer> propertiesLengthMap, int defaultMaxChars, ElasticsearchClient es, int secondBetweenPolling) {
        logPlugins = PluginsConfig.initPluginsFromJson(pluginsJson);
        this.propertiesLengthMap = propertiesLengthMap;
        this.defaultMaxChars = defaultMaxChars;
        this.es = es;
        new Thread(() -> {
            indexMetadataTaskToTimbermill();
            try {
                while (keepRunning) {
                    long l1 = System.currentTimeMillis();
                    try {
                        retrieveAndIndex();
                    } catch (RuntimeException e) {
                        LOG.error("Error was thrown from TaskIndexer:", e);
                    } finally {
                        long l2 = System.currentTimeMillis();
                        long timeToSleep = (secondBetweenPolling * 1000) - (l2 - l1);
                        Thread.sleep(Math.max(timeToSleep, 0));
                    }
                }
                stoppedRunning = true;
            }catch (InterruptedException ignore){
                LOG.info("Timbermill server was interrupted, exiting");
            }
        }).start();
    }

    public void close() {
        keepRunning = false;
        while(!stoppedRunning){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {

            }
        }
        es.close();
    }

    private void retrieveAndIndex() {
        LOG.info("------------------ Batch Start ------------------");
        List<Event> events = new ArrayList<>();
        eventsQueue.drainTo(events, 100);
        DateTime taskIndexerStartTime = new DateTime();
        trimAllStrings(events);
        List<Event> heartbeatEvents = events.stream().filter(e -> (e.getName() != null) && e.getName().equals(Constants.HEARTBEAT_TASK)).collect(toList());
        for (Event e: heartbeatEvents){
            Task heartbeatTask = new Task();
            heartbeatTask.update(e);
            this.es.indexTaskToMetaDataIndex(heartbeatTask, e.getTaskId());
        }

        List<Event> timbermillEvents = events.stream().filter(e -> (e.getName() == null) || !e.getName().equals(Constants.HEARTBEAT_TASK)).collect(toList());

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
                    LOG.info("All {} missing event were retrieved from elasticsearch..", missingEvents.size());
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
            String taskName = "timbermill_plugin";
            for (TaskLogPlugin plugin : logPlugins) {

                Task t = new Task();
                try {
                    t.setName(taskName);
                    t.setPrimary(true);
                    t.setStartTime(new DateTime());
                    t.getString().put("pluginName", plugin.getName());
                    t.getString().put("pluginClass", plugin.getClass().getSimpleName());

                    plugin.apply(events, tasks);

                    t.setEndTime(new DateTime());
                    t.setTotalDuration(t.getEndTime().getMillis() - t.getStartTime().getMillis());
                    t.setStatus(TaskStatus.SUCCESS);
                } catch (Exception ex) {
                    t.getText().put("error", ex.toString());
                    t.setStatus(TaskStatus.ERROR);
                    LOG.error("error in plugin" + plugin, ex);
                }
                String taskId = taskName + '_' + plugin.toString().replace(' ', '_') + "_" + pluginsStart;
                es.indexTaskToMetaDataIndex(t, taskId);
            }
        } catch (Exception ex) {
            LOG.error("Error running plugins", ex);
        }
        long pluginsEnd = System.currentTimeMillis();
        return pluginsEnd - pluginsStart;
    }

    private void trimAllStrings(List<Event> events) {
        events.forEach(e -> {
            e.setStrings(getTrimmedLongValues(e.getStrings(), "string"));
            e.setTexts(getTrimmedLongValues(e.getTexts(), "text"));
            e.setGlobals(getTrimmedLongValues(e.getGlobals(), "global"));
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
        Pair<String, Task> metadataTaskPair = createMetadataTask(taskIndexerStartTime, timbermillEventSize, missingStartEventsSize, missingParentEventsSize, previouslyIndexedTasksSize, tasksToIndexSize);
        Task metadataTask = metadataTaskPair.getRight();
        metadataTask.setStatus(TaskStatus.ERROR);
        Map<String, String> exceptionMap = new HashMap<>();
        exceptionMap.put("exception", ExceptionUtils.getStackTrace(e));
        metadataTask.setText(exceptionMap);
        es.indexTaskToMetaDataIndex(metadataTask, metadataTaskPair.getLeft());
    }

    private void indexMetadataTask(DateTime taskIndexerStartTime, Integer timbermillEventSize, Integer missingStartEventsSize, Integer missingParentEventsSize,
                                   Integer previouslyIndexedTasksSize, Integer tasksToIndexSize, long pluginsDuration) {

        Pair<String, Task> metadataTaskPair = createMetadataTask(taskIndexerStartTime, timbermillEventSize, missingStartEventsSize, missingParentEventsSize, previouslyIndexedTasksSize, tasksToIndexSize);
        Task metadataTask = metadataTaskPair.getRight();
        metadataTask.setStatus(TaskStatus.SUCCESS);
        metadataTask.getMetric().put("pluginsDuration", pluginsDuration);
        es.indexTaskToMetaDataIndex(metadataTask, metadataTaskPair.getLeft());
    }

    private static Pair<String, Task> createMetadataTask(DateTime taskIndexerStartTime, Integer timbermillEventSize, Integer missingStartEventsSize, Integer missingParentEventsSize,
                                                         Integer previouslyIndexedTasksSize,
                                                         Integer tasksToIndexSize) {
        DateTime taskIndexerEndTime = new DateTime();

        Task metadataTask = new Task();
        metadataTask.setName("timbermill_index");
        metadataTask.setPrimary(true);
        Map<String, Number> taskIndexerMetrics = new HashMap<>();

        taskIndexerMetrics.put(INCOMING_EVENTS, timbermillEventSize);
        taskIndexerMetrics.put(MISSING_START_EVENTS, missingStartEventsSize);
        taskIndexerMetrics.put(MISSING_PARENTS_EVENTS, missingParentEventsSize);
        taskIndexerMetrics.put(PREV_INDEXED_TASKS_FETCHED, previouslyIndexedTasksSize);
        taskIndexerMetrics.put(INDEXED_TASKS, tasksToIndexSize);

        metadataTask.setMetric(taskIndexerMetrics);

        metadataTask.setStartTime(taskIndexerStartTime);
        metadataTask.setEndTime(taskIndexerEndTime);
        metadataTask.setTotalDuration(taskIndexerEndTime.getMillis() - taskIndexerStartTime.getMillis());
        String taskId = metadataTask.getName() + '-' + taskIndexerStartTime.getMillis();
        return org.apache.commons.lang3.tuple.Pair.of(taskId, metadataTask);
    }

    private static Set<String> getMissingStartEvents(Collection<Event> events) {
        Set<String> startEventTasks = events.stream()
                .filter(e -> (e.getEventType() == EventType.START) || (e.getEventType() == EventType.SPOT))
                .map(e -> e.getTaskId()).collect(Collectors.toCollection(LinkedHashSet::new));

        return Sets.difference(getAllTaskIds(events), startEventTasks);
    }

    private static Set<String> getMissingParentEvents(Collection<Event> events) {
        Set<String> allEventParentTasks = events.stream()
                .filter(e -> e.getParentId() != null)
                .map(e -> e.getParentId()).collect(Collectors.toCollection(LinkedHashSet::new));

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
            String taskId = idToEventEntry.getKey();
            if (previouslyIndexedTasks.containsKey(taskId)) {
                t = previouslyIndexedTasks.get(taskId);
            }
            else if (tasksToIndex.containsKey(taskId)){
                t = tasksToIndex.get(taskId);
            }
            else {
                t = new Task();
            }

            t.update(idToEventEntry.getValue());

            //Start event was not indexed
            if (t.isStartTimeMissing()){
                LOG.error("Task id {} start event is missing from the elasticsearch", taskId);
                t.setStatus(TaskStatus.CORRUPTED);
                t.setStartTime(t.getEndTime());
            }
            t.setEnv(es.getEnv());
            tasksToIndex.put(taskId, t);
        }

        /*
         * Compute origins and down merge parameters from parent
         */
        for (String tid : getStartEventsTaskIds(events)) {
            Task t = tasksToIndex.get(tid);
            List<String> origins = new ArrayList<>();
            String parentId = t.getParentId();
            if (parentId != null) {
                Task parentTask = getParentTask(previouslyIndexedTasks, tasksToIndex, parentId);
                if (parentTask != null) {
                    t.setPrimaryId(parentTask.getPrimaryId());
                    for (Map.Entry<String, Object> entry : parentTask.getGlobal().entrySet()) {
                        t.getGlobal().putIfAbsent(entry.getKey(), entry.getValue());
                    }
                    if((parentTask.getParentsPath() != null) && !parentTask.getParentsPath().isEmpty()) {
                        origins.addAll(parentTask.getParentsPath());
                    }
                    origins.add(parentTask.getName());
                }
                else {
                    t.setPrimaryId(tid);
                    t.setParentId(null);
                }
            }
            if(!origins.isEmpty()) {
                t.setParentsPath(origins);
            }
        }

        for (String tid : getEndEventsTaskIds(events)){
            Task t = tasksToIndex.get(tid);
            String parentId = t.getParentId();
            long totalDuration = t.getEndTime().getMillis() - t.getStartTime().getMillis();
            t.setTotalDuration(totalDuration);
            if (parentId != null) {
                Task parentTask = getParentTask(previouslyIndexedTasks, tasksToIndex, parentId);
                if( parentTask != null) {
                    if (!tasksToIndex.containsKey(parentId)) {
                        tasksToIndex.put(parentId, parentTask);
                    }
                }
                else{
                    LOG.error("Parent task with id {} for child task with id {} could not be found in elasticsearch", parentId, tid);
                }
            }
        }

        return tasksToIndex;
    }

    private static Task getParentTask(Map<String, Task> previouslyIndexedTasks, Map<String, Task> tasksToIndex, String parenId) {
        Task parentTask = tasksToIndex.get(parenId);
        if (parentTask == null) {
            parentTask = previouslyIndexedTasks.get(parenId);
        }
        return parentTask;
    }

    private void indexMetadataTaskToTimbermill() {
        String taskId = "timbermill_server_startup" + '-' + new DateTime().getMillis();
        Task task = new Task();
        task.setName("timbermill_server_startup");
        task.setPrimary(true);
        task.setStartTime(new DateTime());
        task.setEndTime(new DateTime());
        es.indexTaskToMetaDataIndex(task, taskId);
    }
}

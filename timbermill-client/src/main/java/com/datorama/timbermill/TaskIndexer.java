package com.datorama.timbermill;


import com.datorama.timbermill.common.Constants;
import com.datorama.timbermill.pipe.LocalOutputPipeConfig;
import com.datorama.timbermill.plugins.PluginsConfig;
import com.datorama.timbermill.plugins.TaskLogPlugin;
import com.datorama.timbermill.unit.*;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.elasticsearch.ElasticsearchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static com.datorama.timbermill.common.Constants.GSON;
import static com.datorama.timbermill.unit.Task.TaskStatus;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

public class TaskIndexer {

    private static final Logger LOG = LoggerFactory.getLogger(TaskIndexer.class);

    private final ElasticsearchClient es;
    private final Collection<TaskLogPlugin> logPlugins;
    private final Map<String, Integer> propertiesLengthMap;
    private final int defaultMaxChars;

    public TaskIndexer(LocalOutputPipeConfig config) {
        logPlugins = PluginsConfig.initPluginsFromJson(config.getPlugingJson());
        this.propertiesLengthMap = config.getPropertiesLengthMap();
        this.defaultMaxChars = config.getDefaultMaxChars();
        this.es = new ElasticsearchClient(config.getEnv(), config.getElasticUrl(), config.getIndexBulkSize(), config.getDaysRotation(), config.getAwsRegion());
    }

    public TaskIndexer(String pluginsJson, String propertiesLengthJson, int defaultMaxChars, String env, String elasticUrl, int daysRotation, String awsRegion, int indexBulkSize) {
        logPlugins = PluginsConfig.initPluginsFromJson(pluginsJson);
        this.propertiesLengthMap = new Gson().fromJson(propertiesLengthJson, Map.class);
        this.defaultMaxChars = defaultMaxChars;
        this.es = new ElasticsearchClient(env, elasticUrl, indexBulkSize, daysRotation, awsRegion);
    }

    public void retrieveAndIndex(List<Event> events) {
        LOG.info("------------------ Batch Start ------------------");
        ZonedDateTime taskIndexerStartTime = ZonedDateTime.now();
        trimAllStrings(events);
        List<Event> heartbeatEvents = events.stream().filter(e -> (e.getName() != null) && e.getName().equals(Constants.HEARTBEAT_TASK)).collect(toList());
        for (Event e: heartbeatEvents){
            HeartbeatEvent heartbeatEvent = new HeartbeatEvent(e);
            this.es.indexMetaDataEvent(e.getStartTime(), GSON.toJson(heartbeatEvent));
        }

        List<Event> timbermillEvents = events.stream().filter(e -> (e.getName() == null) || !e.getName().equals(Constants.HEARTBEAT_TASK)).collect(toList());

        LOG.info("{} events retrieved from pipe", timbermillEvents.size());
        Set<String> missingParentEvents = getMissingParentEvents(timbermillEvents);

        Map<String, Task> previouslyIndexedTasks = new HashMap<>();
        try {
            if (!missingParentEvents.isEmpty()) {
                int missingParentNum = missingParentEvents.size();
                LOG.info("{} missing parent events for current batch", missingParentNum);
                previouslyIndexedTasks = this.es.fetchIndexedTasks(missingParentEvents);
                if (missingParentNum == previouslyIndexedTasks.size()) {
                    LOG.info("All {} missing event were retrieved from elasticsearch..", missingParentNum);
                } else {
                    Sets.SetView<String> notFoundEvents = Sets.difference(missingParentEvents, previouslyIndexedTasks.keySet());

                    for (String notFoundEvent : notFoundEvents) {
                        for (Event event : timbermillEvents) {
                            if (notFoundEvent.equals(event.getPrimaryId())){
                                event.setPrimaryId(null);
                            }
                        }
                    }


                    String joinString = StringUtils.join(notFoundEvents, ",");
                    LOG.warn("{} missing events were not retrieved from elasticsearch. Events [{}]", missingParentNum - previouslyIndexedTasks.size(), joinString);
                }
            }

            long pluginsDuration = applyPlugins(timbermillEvents);

            enrichEvents(timbermillEvents, previouslyIndexedTasks);
            es.index(timbermillEvents);
            if (!timbermillEvents.isEmpty()) {
                LOG.info("{} tasks were indexed to elasticsearch", timbermillEvents.size());
                indexMetadataTask(taskIndexerStartTime, timbermillEvents.size(), missingParentEvents.size(), pluginsDuration);
            }
            LOG.info("------------------ Batch End ------------------");
        } catch (ElasticsearchException e){
            indexFailedMetadataTask(taskIndexerStartTime, timbermillEvents.size(), previouslyIndexedTasks.size(), e);
        }
    }

    private void enrichEvents(List<Event> events, Map<String, Task> previouslyIndexedTasks) {

        /*
         * Compute origins and down merge parameters from parent
         */
        List<Event> startEvents = events.stream().filter(e -> e.isStartEvent()).collect(toList());
        for (Event event : startEvents) {
            List<String> parentsPath = new ArrayList<>();
            String parentId = event.getParentId();
            if (parentId != null) {
                ParentProperties parentProperties = getParentProperties(previouslyIndexedTasks, events.stream().collect(groupingBy(e -> e.getTaskId())), parentId);

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

    private long applyPlugins(List<Event> events) {
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
                es.indexMetaDataEvent(startTime, GSON.toJson(pluginApplierEvent));
            }
        } catch (Exception ex) {
            LOG.error("Error running plugins", ex);
        }
        long pluginsEnd = System.currentTimeMillis();
        return pluginsEnd - pluginsStart;
    }

    private void trimAllStrings(List<Event> events) {
        events.forEach(e -> {
            e.setTrimmedStrings(getTrimmedLongValues(e.getStrings(), "string"));
            e.setTrimmedTexts(getTrimmedLongValues(e.getTexts(), "text"));
            e.setTrimmedContext(getTrimmedLongValues(e.getContext(), "ctx"));
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
                                         Integer fetchedAmount, Exception e) {
        ZonedDateTime taskIndexerEndTime = ZonedDateTime.now();
        long duration = taskIndexerEndTime.toInstant().toEpochMilli() - taskIndexerStartTime.toInstant().toEpochMilli();

        IndexEvent indexEvent = new IndexEvent(eventsAmount, fetchedAmount, taskIndexerStartTime, taskIndexerEndTime, duration, TaskStatus.ERROR, ExceptionUtils.getStackTrace(e), null);
        es.indexMetaDataEvent(taskIndexerStartTime, GSON.toJson(indexEvent));
    }

    private void indexMetadataTask(ZonedDateTime taskIndexerStartTime, Integer eventsAmount, Integer fetchedAmount, long pluginsDuration) {

        ZonedDateTime taskIndexerEndTime = ZonedDateTime.now();
        long duration = taskIndexerEndTime.toInstant().toEpochMilli() - taskIndexerStartTime.toInstant().toEpochMilli();

        IndexEvent indexEvent = new IndexEvent(eventsAmount, fetchedAmount, taskIndexerStartTime, taskIndexerEndTime, duration, TaskStatus.SUCCESS, null, pluginsDuration);
        es.indexMetaDataEvent(taskIndexerStartTime, GSON.toJson(indexEvent));
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

    private static ParentProperties getParentProperties(Map<String, Task> previouslyIndexedTasks, Map<String, List<Event>> currentBatchEvents, String parentId) {

        Map<String, String> context = Maps.newHashMap();
        String primaryId = null;
        List<String> parentPath = null;
        String parentName = null;
        Task indexedTask = previouslyIndexedTasks.get(parentId);
        if (indexedTask != null){
            primaryId = indexedTask.getPrimaryId();
            context = indexedTask.getCtx();
            parentPath = indexedTask.getParentsPath();
            parentName = indexedTask.getName();
        }

        List<Event> previousEvents = currentBatchEvents.get(parentId);
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

    public void indexStartupEvent() {
        ZonedDateTime time = ZonedDateTime.now();
        LocalStartupEvent localStartupEvent = new LocalStartupEvent(time);
        es.indexMetaDataEvent(time, GSON.toJson(localStartupEvent));
    }

    public void close() {
        es.close();
    }

    private static class ParentProperties{

        private final String primaryId;
        private final Map<String, String> context;
        private final List<String> parentPath;
        private String parentName;

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

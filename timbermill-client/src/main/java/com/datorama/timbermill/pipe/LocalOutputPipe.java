package com.datorama.timbermill.pipe;

import com.datorama.timbermill.ElasticsearchParams;
import com.datorama.timbermill.TaskIndexer;
import com.datorama.timbermill.unit.Event;
import org.elasticsearch.ElasticsearchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

public class LocalOutputPipe implements EventOutputPipe {

    private static final int EVENT_QUEUE_CAPACITY = 1000000;
    private static final int THREAD_SLEEP = 2000;
    private final BlockingQueue<Event> eventsQueue = new ArrayBlockingQueue<>(EVENT_QUEUE_CAPACITY);
    
    private TaskIndexer taskIndexer;
    private boolean keepRunning = true;
    private boolean stoppedRunning = false;

    private static final Logger LOG = LoggerFactory.getLogger(LocalOutputPipe.class);

    private LocalOutputPipe() {
    }

    private LocalOutputPipe(Builder builder) {
        if (builder.elasticUrl == null){
            throw new ElasticsearchException("Must enclose an Elasticsearch URL");
        }
        ElasticsearchParams elasticsearchParams = new ElasticsearchParams(builder.elasticUrl, builder.defaultMaxChars, builder.daysRotation, builder.awsRegion, builder.indexingThreads,
                builder.elasticUser, builder.elasticPassword,
                builder.indexBulkSize, builder.pluginsJson, builder.propertiesLengthMap,
                builder.maxCacheSize, builder.maxCacheHoldTimeMinutes);
        taskIndexer = new TaskIndexer(elasticsearchParams);

        startWorkingThread();
    }

    private void startWorkingThread() {
        Runnable eventsHandler = () -> {
            LOG.info("Timbermill has started");
            while (keepRunning) {
                while (!eventsQueue.isEmpty()) {
                    try {
                        Collection<Event> events = new ArrayList<>();
                        eventsQueue.drainTo(events);
                        Map<String, List<Event>> eventsPerEnvMap = events.stream().collect(Collectors.groupingBy(Event::getEnv));
                        for (Map.Entry<String, List<Event>> eventsPerEnv : eventsPerEnvMap.entrySet()) {
                            String env = eventsPerEnv.getKey();
                            Collection<Event> currentEvents = eventsPerEnv.getValue();
                            taskIndexer.retrieveAndIndex(currentEvents, env);
                        }
                        Thread.sleep(THREAD_SLEEP);
                    } catch (RuntimeException | InterruptedException e) {
                        LOG.error("Error was thrown from TaskIndexer:", e);
                    }
                }
            }
            stoppedRunning = true;
        };
        Thread workingThread = new Thread(eventsHandler);
        workingThread.start();
    }

    @Override
    public void send(Event e){
        eventsQueue.add(e);
    }

    public void close() {
        LOG.info("Gracefully shutting down Timbermill Server.");
        taskIndexer.close();
        keepRunning = false;
        while(!stoppedRunning){
            try {
                Thread.sleep(THREAD_SLEEP);
            } catch (InterruptedException ignored) {
            }
        }
        LOG.info("Timbermill server was shut down.");
    }

    public static class Builder {

        private int maxCacheHoldTimeMinutes = 60;
        private int maxCacheSize = 10000;
        private String elasticUrl = null;
        private String pluginsJson = "[]";
        private Map<String, Integer> propertiesLengthMap = Collections.EMPTY_MAP;
        private int defaultMaxChars = 1000000;
        private int daysRotation = 0;
        private int indexBulkSize = 2097152;
        private int indexingThreads = 1;
        private String elasticUser = null;
        private String awsRegion = null;
        private String elasticPassword = null;

        public Builder url(String elasticUrl) {
            this.elasticUrl = elasticUrl;
            return this;
        }

        public Builder pluginsJson(String pluginsJson) {
            this.pluginsJson = pluginsJson;
            return this;
        }

        public Builder propertiesLengthMap(Map<String, Integer> propertiesLengthMap) {
            this.propertiesLengthMap = propertiesLengthMap;
            return this;
        }

        public Builder defaultMaxChars(int defaultMaxChars) {
            this.defaultMaxChars = defaultMaxChars;
            return this;
        }

        public Builder daysRotation(int daysRotation) {
            this.daysRotation = daysRotation;
            return this;
        }

        public Builder awsRegion(String awsRegion) {
            this.awsRegion = awsRegion;
            return this;
        }

        public Builder indexBulkSize(int indexBulkSize) {
            this.indexBulkSize = indexBulkSize;
            return this;
        }

        public Builder indexingThreads(int indexingThreads) {
            this.indexingThreads = indexingThreads;
            return this;
        }

        public Builder elasticUser(String elasticUser) {
            this.elasticUser = elasticUser;
            return this;
        }

        public Builder elasticPassword(String elasticPassword) {
            this.elasticPassword = elasticPassword;
            return this;
        }

        public Builder maxCacheSize(int maxCacheSize) {
            this.maxCacheSize = maxCacheSize;
            return this;
        }

        public Builder maxCacheHoldTimeMinutes(int maxCacheHoldTimeMinutes) {
            this.maxCacheHoldTimeMinutes = maxCacheHoldTimeMinutes;
            return this;
        }

        public LocalOutputPipe build() {
            return new LocalOutputPipe(this);
        }
    }
}

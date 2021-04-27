package com.datorama.oss.timbermill.pipe;

import com.datorama.oss.timbermill.*;
import com.datorama.oss.timbermill.common.ElasticsearchUtil;
import com.datorama.oss.timbermill.common.KamonConstants;
import com.datorama.oss.timbermill.common.persistence.PersistenceHandler;
import com.datorama.oss.timbermill.common.persistence.PersistenceHandlerUtil;
import com.datorama.oss.timbermill.common.redis.RedisService;
import com.datorama.oss.timbermill.cron.CronsRunner;
import com.datorama.oss.timbermill.unit.Event;
import org.elasticsearch.ElasticsearchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class LocalOutputPipe implements EventOutputPipe {

    private static final int EVENT_QUEUE_CAPACITY = 1000000;

    private final BlockingQueue<Event> buffer = new ArrayBlockingQueue<>(EVENT_QUEUE_CAPACITY);
    private BlockingQueue<Event> overflowedQueue = new ArrayBlockingQueue<>(EVENT_QUEUE_CAPACITY);
    private PersistenceHandler persistenceHandler;
    private ElasticsearchClient esClient;
    private TaskIndexer taskIndexer;
    private final CronsRunner cronsRunner;
    private boolean keepRunning = true;
    private boolean stoppedRunning = false;
    private static final Logger LOG = LoggerFactory.getLogger(LocalOutputPipe.class);

    private LocalOutputPipe(Builder builder) {
        if (builder.elasticUrl == null){
            throw new ElasticsearchException("Must enclose an Elasticsearch URL");
        }

        RedisService redisService = new RedisService(builder.redisHost, builder.redisPort, builder.redisPass, builder.redisMaxMemory, builder.redisMaxMemoryPolicy, builder.redisUseSsl, builder.redisGetSize, builder.redisPoolMinIdle, builder.redisPoolMaxIdle, builder.redisPoolMaxTotal, builder.redisMaxTries);
        Map<String, Object> params = PersistenceHandler.buildPersistenceHandlerParams(builder.maxFetchedBulksInOneTime, builder.maxFetchedEventsInOneTime, builder.maxInsertTries, builder.locationInDisk, builder.redisTtlInSeconds, redisService);
        persistenceHandler = PersistenceHandlerUtil.getPersistenceHandler(builder.persistenceHandlerStrategy, params);
        esClient = new ElasticsearchClient(builder.elasticUrl, builder.indexBulkSize, builder.indexingThreads, builder.awsRegion, builder.elasticUser, builder.elasticPassword,
                builder.maxIndexAge, builder.maxIndexSizeInGB, builder.maxIndexDocs, builder.numOfElasticSearchActionsTries, builder.maxBulkIndexFetched, builder.searchMaxSize, persistenceHandler,
                builder.numberOfShards, builder.numberOfReplicas, builder.maxTotalFields, builder.bulker, builder.scrollLimitation, builder.scrollTimeoutSeconds, builder.fetchByIdsPartitions,
                builder.expiredMaxIndicesToDeleteInParallel);

        LocalCacheConfig localCacheConfig = new LocalCacheConfig(builder.maximumTasksCacheWeight, builder.maximumOrphansCacheWeight);
        RedisCacheConfig redisCacheConfig = new RedisCacheConfig(redisService, builder.redisTtlInSeconds);
        taskIndexer = new TaskIndexer(builder.pluginsJson, builder.daysRotation, esClient, builder.timbermillVersion,
                localCacheConfig, builder.cacheStrategy,
                redisCacheConfig);
        cronsRunner = new CronsRunner();
        cronsRunner.runCrons(builder.bulkPersistentFetchCronExp, builder.eventsPersistentFetchCronExp, persistenceHandler, esClient,
                builder.deletionCronExp, buffer, overflowedQueue,
                builder.mergingCronExp, redisService);
        startQueueSpillerThread();
        startWorkingThread();
    }

    private void startQueueSpillerThread() {
        Thread spillerThread = new Thread(() -> {
            LOG.info("Starting Queue Spiller Thread");
            while (keepRunning) {
                persistenceHandler.spillOverflownEvents(overflowedQueue);
                try {
                    Thread.sleep(ElasticsearchUtil.THREAD_SLEEP);
                } catch (InterruptedException e) {
                    LOG.error("InterruptedException was thrown from TaskIndexer:", e);
                }
            }
        });
        spillerThread.start();
    }

    private void startWorkingThread() {
        Thread workingThread = new Thread(() -> {
            LOG.info("Timbermill has started");
            while (keepRunning) {
                ElasticsearchUtil.drainAndIndex(buffer, taskIndexer);
            }
            stoppedRunning = true;
        });
        workingThread.start();
    }

    @Override
    public void send(Event event){
        pushEventToQueues(persistenceHandler, buffer, overflowedQueue, event);
    }

    public static void pushEventToQueues(PersistenceHandler persistenceHandler, BlockingQueue<Event> eventsQueue, BlockingQueue<Event> overflowedQueue, Event event) {
        if(!eventsQueue.offer(event)){
            if (!overflowedQueue.offer(event)){
                persistenceHandler.spillOverflownEvents(overflowedQueue);
                if (!overflowedQueue.offer(event)) {
                    LOG.error("OverflowedQueue is full, event {} was discarded", event.getTaskId());
                }
            }
            else {
                KamonConstants.MESSAGES_IN_OVERFLOWED_QUEUE_RANGE_SAMPLER.withoutTags().increment();
            }
        }
        else{
            KamonConstants.MESSAGES_IN_INPUT_QUEUE_RANGE_SAMPLER.withoutTags().increment();
        }
    }

    public void close() {
        LOG.info("Gracefully shutting down Timbermill Server.");
        keepRunning = false;
        while(!stoppedRunning){
            try {
                Thread.sleep(ElasticsearchUtil.THREAD_SLEEP);
            } catch (InterruptedException ignored) {
            }
        }
        if (persistenceHandler != null){
            persistenceHandler.close();
        }
        taskIndexer.close();
        cronsRunner.close();
        LOG.info("Timbermill server was shut down.");
    }

    @Override public int getCurrentBufferSize() {
        return buffer.size();
    }

    public ElasticsearchClient getEsClient() {
        return esClient;
    }

    public BlockingQueue<Event> getBuffer() {
        return buffer;
    }

    public BlockingQueue<Event> getOverflowedQueue() {
        return overflowedQueue;
    }

    public PersistenceHandler getPersistenceHandler() {
        return persistenceHandler;
    }

    public static class Builder {

        Bulker bulker;
        //DEFAULTS
        private String redisHost = "localhost";
        private int redisPort = 6379;
        private String redisPass = "";
        private String cacheStrategy = "none";
        private String redisMaxMemory = "";
        private String redisMaxMemoryPolicy = "";
        private int redisTtlInSeconds = 604800;
        private int redisGetSize = 100;
        private boolean redisUseSsl = false;
        private int redisPoolMinIdle = 10;
        private int redisPoolMaxIdle = 10;
        private int redisPoolMaxTotal = 10;
        private int redisMaxTries = 3;
        private int maximumTasksCacheWeight = 1000000000;
        private int maximumOrphansCacheWeight = 1000000000;
        private int searchMaxSize = 1000;
        private int maxBulkIndexFetched = 3;
        private int numOfElasticSearchActionsTries = 3;
        private String elasticUrl = null;
        private String pluginsJson = "[]";
        private int daysRotation = 90;
        private int indexBulkSize = 200000;
        private int indexingThreads = 1;
        private String elasticUser = null;
        private String awsRegion = null;
        private String elasticPassword = null;
        private int numberOfShards = 10;
        private int numberOfReplicas = 1;
        private long maxIndexAge = 7;
        private long maxIndexSizeInGB = 100;
        private int maxTotalFields = 4000;
        private long maxIndexDocs = 1000000000;
        private String deletionCronExp = "0 0 12 1/1 * ? *";
        private String mergingCronExp = "0 0/1 * 1/1 * ? *";
        private String bulkPersistentFetchCronExp = "0 0/10 * 1/1 * ? *";
        private String eventsPersistentFetchCronExp = "0 0/5 * 1/1 * ? *";
        private String persistenceHandlerStrategy = "redis";
        private int maxFetchedBulksInOneTime = 10;
        private int maxFetchedEventsInOneTime = 10;
        private int maxInsertTries = 3;
        private String locationInDisk = "/tmp";
        private int scrollLimitation = 1000;
        private int scrollTimeoutSeconds = 60;
        private int fetchByIdsPartitions = 10000;
        private int expiredMaxIndicesToDeleteInParallel = 2;
        private String timbermillVersion = "";

        public Builder url(String elasticUrl) {
            this.elasticUrl = elasticUrl;
            return this;
        }

        public Builder pluginsJson(String pluginsJson) {
            this.pluginsJson = pluginsJson;
            return this;
        }

        public Builder numberOfShards(int numberOfShards) {
            this.numberOfShards = numberOfShards;
            return this;
        }

        public Builder deletionCronExp(String deletionCronExp) {
            this.deletionCronExp = deletionCronExp;
            return this;
        }

        public Builder numberOfReplicas(int numberOfReplicas) {
            this.numberOfReplicas = numberOfReplicas;
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

        public Builder maxIndexAge(long maxIndexAge) {
            this.maxIndexAge = maxIndexAge;
            return this;
        }

        public Builder maxIndexSizeInGB(long maxIndexSizeInGB) {
            this.maxIndexSizeInGB = maxIndexSizeInGB;
            return this;
        }

        public Builder maxIndexDocs(long maxIndexDocs) {
            this.maxIndexDocs = maxIndexDocs;
            return this;
        }

        public Builder mergingCronExp(String mergingCronExp) {
            this.mergingCronExp = mergingCronExp;
            return this;
        }

        public Builder numOfElasticSearchActionsTries(int numOfElasticSearchActionsTries) {
            this.numOfElasticSearchActionsTries = numOfElasticSearchActionsTries;
            return this;
        }

        public Builder persistenceHandlerStrategy(String persistenceHandlerStrategy) {
            this.persistenceHandlerStrategy = persistenceHandlerStrategy;
            return this;
        }

        public Builder maxFetchedBulksInOneTime(int maxFetchedBulksInOneTime) {
            this.maxFetchedBulksInOneTime = maxFetchedBulksInOneTime;
            return this;
        }

        public Builder maxInsertTries(int maxInsertTries) {
            this.maxInsertTries = maxInsertTries;
            return this;
        }

        public Builder locationInDisk(String locationInDisk) {
            this.locationInDisk = locationInDisk;
            return this;
        }

        public Builder maxBulkIndexFetched(int maxBulkIndexFetched) {
            this.maxBulkIndexFetched = maxBulkIndexFetched;
            return this;
        }

        public Builder searchMaxSize(int searchMaxSize) {
            this.searchMaxSize = searchMaxSize;
            return this;
        }

        public Builder maximumTasksCacheWeight(int maximumTasksCacheWeight) {
            this.maximumTasksCacheWeight = maximumTasksCacheWeight;
            return this;
        }

        public Builder maximumOrphansCacheWeight(int maximumOrphansCacheWeight) {
            this.maximumOrphansCacheWeight = maximumOrphansCacheWeight;
            return this;
        }

        public Builder bulkPersistentFetchCronExp(String bulkPersistentFetchCronExp) {
            this.bulkPersistentFetchCronExp = bulkPersistentFetchCronExp;
            return this;
        }

        public Builder eventsPersistentFetchCronExp(String eventsPersistentFetchCronExp) {
            this.eventsPersistentFetchCronExp = eventsPersistentFetchCronExp;
            return this;
        }

        public Builder timbermillVersion(String timbermillVersion) {
            this.timbermillVersion = timbermillVersion;
            return this;
        }

        public Builder redisMaxMemory(String redisMaxMemory) {
            this.redisMaxMemory = redisMaxMemory;
            return this;
        }

        public Builder redisMaxMemoryPolicy(String redisMaxMemoryPolicy) {
            this.redisMaxMemoryPolicy = redisMaxMemoryPolicy;
            return this;
        }

        public Builder cacheStrategy(String cacheStrategy) {
            this.cacheStrategy = cacheStrategy;
            return this;
        }

        public Builder redisHost(String redisHost) {
            this.redisHost = redisHost;
            return this;
        }

        public Builder redisPort(int redisPort) {
            this.redisPort = redisPort;
            return this;
        }

        public Builder redisPass(String redisPass) {
            this.redisPass = redisPass;
            return this;
        }

        public Builder redisUseSsl(boolean redisUseSsl) {
            this.redisUseSsl = redisUseSsl;
            return this;
        }

        public Builder redisTtlInSeconds(int redisTtlInSeconds) {
            this.redisTtlInSeconds = redisTtlInSeconds;
            return this;
        }

        public Builder redisGetSize(int redisGetSize) {
            this.redisGetSize = redisGetSize;
            return this;
        }

        public Builder redisPoolMinIdle(int redisPoolMinIdle) {
            this.redisPoolMinIdle = redisPoolMinIdle;
            return this;
        }

        public Builder redisPoolMaxTotal(int redisPoolMaxTotal) {
            this.redisPoolMaxTotal = redisPoolMaxTotal;
            return this;
        }

        public Builder redisMaxTries(int redisMaxTries) {
            this.redisMaxTries = redisMaxTries;
            return this;
        }

        //Tests
        public Builder bulker(Bulker bulker) {
            this.bulker = bulker;
            return this;
        }

        public Builder scrollLimitation(int scrollLimitation) {
            this.scrollLimitation = scrollLimitation;
            return this;
        }

        public Builder scrollTimeoutSeconds(int scrollTimeoutSeconds) {
            this.scrollTimeoutSeconds = scrollTimeoutSeconds;
            return this;
        }

        public Builder fetchByIdsPartitions(int fetchByIdsPartitions) {
            this.fetchByIdsPartitions = fetchByIdsPartitions;
            return this;
        }

        public Builder expiredMaxIndicesToDeleteInParallel(int expiredMaxIndicesToDeleteInParallel) {
            this.expiredMaxIndicesToDeleteInParallel = expiredMaxIndicesToDeleteInParallel;
            return this;
        }

        public LocalOutputPipe build() {
            return new LocalOutputPipe(this);
        }
    }
}

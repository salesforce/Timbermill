package com.datorama.oss.timbermill.pipe;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.elasticsearch.ElasticsearchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.oss.timbermill.Bulker;
import com.datorama.oss.timbermill.ElasticsearchClient;
import com.datorama.oss.timbermill.TaskIndexer;
import com.datorama.oss.timbermill.common.ElasticsearchUtil;
import com.datorama.oss.timbermill.common.disk.DiskHandler;
import com.datorama.oss.timbermill.common.disk.DiskHandlerUtil;
import com.datorama.oss.timbermill.cron.CronsRunner;
import com.datorama.oss.timbermill.unit.Event;

public class LocalOutputPipe implements EventOutputPipe {

    private static final int EVENT_QUEUE_CAPACITY = 1000000;

    private final BlockingQueue<Event> buffer = new ArrayBlockingQueue<>(EVENT_QUEUE_CAPACITY);
    private BlockingQueue<Event> overflowedQueue = new ArrayBlockingQueue<>(EVENT_QUEUE_CAPACITY);
    private DiskHandler diskHandler;
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

        Map<String, Object> params = DiskHandler.buildDiskHandlerParams(builder.maxFetchedBulksInOneTime, builder.maxInsertTries, builder.locationInDisk);
        diskHandler = DiskHandlerUtil.getDiskHandler(builder.diskHandlerStrategy, params);
        esClient = new ElasticsearchClient(builder.elasticUrl, builder.indexBulkSize, builder.indexingThreads, builder.awsRegion, builder.elasticUser, builder.elasticPassword,
                builder.maxIndexAge, builder.maxIndexSizeInGB, builder.maxIndexDocs, builder.numOfElasticSearchActionsTries, builder.maxBulkIndexFetched, builder.searchMaxSize, diskHandler,
                builder.numberOfShards, builder.numberOfReplicas, builder.maxTotalFields, builder.bulker, builder.scrollLimitation, builder.scrollTimeoutSeconds, builder.fetchByIdsPartitions,
                builder.expiredMaxIndicesToDeleteInParallel);

        taskIndexer = new TaskIndexer(builder.pluginsJson, builder.daysRotation, esClient, builder.timbermillVersion,
                builder.maximumOrphansCacheWeight, builder.maximumTasksCacheWeight, builder.cacheStrategy,
                builder.redisHost, builder.redisPort, builder.redisPass, builder.redisMaxMemory, builder.redisMaxMemoryPolicy, builder.redisUseSsl, builder.redisTtlInSeconds, builder.redisGetSize, builder.redisPoolMinIdle, builder.redisPoolMaxTotal);
        cronsRunner = new CronsRunner();
        cronsRunner.runCrons(builder.bulkPersistentFetchCronExp, builder.eventsPersistentFetchCronExp, diskHandler, esClient,
                builder.deletionCronExp, buffer, overflowedQueue,
                builder.mergingCronExp);
        startQueueSpillerThread();
        startWorkingThread();
    }

    private void startQueueSpillerThread() {
        Thread spillerThread = new Thread(() -> {
            LOG.info("Starting Queue Spiller Thread");
            while (keepRunning) {
                diskHandler.spillOverflownEventsToDisk(overflowedQueue);
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
        if(!this.buffer.offer(event)){
            if (!overflowedQueue.offer(event)){
                diskHandler.spillOverflownEventsToDisk(overflowedQueue);
                if (!overflowedQueue.offer(event)) {
                    LOG.error("OverflowedQueue is full, event {} was discarded", event.getTaskId());
                }
            }
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
        if (diskHandler != null){
            diskHandler.close();
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

    public DiskHandler getDiskHandler() {
        return diskHandler;
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
        private int redisGetSize = 10000;
        private boolean redisUseSsl = false;
        private int redisPoolMinIdle = 5;
        private int redisPoolMaxTotal = 10;
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
        private String diskHandlerStrategy = "sqlite";
        private int maxFetchedBulksInOneTime = 10;
        private int maxInsertTries = 10;
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

        public Builder diskHandlerStrategy(String diskHandlerStrategy) {
            this.diskHandlerStrategy = diskHandlerStrategy;
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

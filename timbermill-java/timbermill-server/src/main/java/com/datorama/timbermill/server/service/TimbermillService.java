package com.datorama.timbermill.server.service;

import com.datorama.oss.timbermill.ElasticsearchClient;
import com.datorama.oss.timbermill.TaskIndexer;
import com.datorama.oss.timbermill.common.ElasticsearchUtil;
import com.datorama.oss.timbermill.common.cache.AbstractCacheHandler;
import com.datorama.oss.timbermill.common.cache.CacheConfig;
import com.datorama.oss.timbermill.common.cache.CacheHandlerUtil;
import com.datorama.oss.timbermill.common.persistence.PersistenceHandler;
import com.datorama.oss.timbermill.common.persistence.PersistenceHandlerUtil;
import com.datorama.oss.timbermill.common.ratelimiter.RateLimiterUtil;
import com.datorama.oss.timbermill.common.redis.RedisService;
import com.datorama.oss.timbermill.cron.CronsRunner;
import com.datorama.oss.timbermill.pipe.LocalOutputPipe;
import com.datorama.oss.timbermill.unit.Event;
import com.google.common.cache.LoadingCache;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import io.github.resilience4j.ratelimiter.RateLimiter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Service
public class TimbermillService {

	private static final Logger LOG = LoggerFactory.getLogger(TimbermillService.class);

	private TaskIndexer taskIndexer;
	private BlockingQueue<Event> eventsQueue;
	private BlockingQueue<Event> overflowedQueue;
    private LoadingCache<String, RateLimiter> rateLimiterMap;


	private boolean keepRunning = true;
	private boolean stoppedRunning = false;
	private long terminationTimeout;
	private PersistenceHandler persistenceHandler;
	private CronsRunner cronsRunner = new CronsRunner();
	private int eventsMaxElement;

	private Gson gson = new Gson();

	private static final Type pairType = new TypeToken<ImmutablePair<String, Object>>() {
	}.getType();

	@Autowired
	public TimbermillService(@Value("${INDEX_BULK_SIZE:200000}") Integer indexBulkSize,
							 @Value("${ELASTICSEARCH_URL:http://localhost:9200}") String elasticUrl,
							 @Value("${ELASTICSEARCH_AWS_REGION:}") String awsRegion,
							 @Value("${ELASTICSEARCH_USER:}") String elasticUser,
							 @Value("${ELASTICSEARCH_PASSWORD:}") String elasticPassword,
							 @Value("${ELASTICSEARCH_NUMBER_OF_SHARDS:10}") int numberOfShards,
							 @Value("${ELASTICSEARCH_NUMBER_OF_REPLICAS:1}") int numberOfReplicas,
							 @Value("${ELASTICSEARCH_INDEX_MAX_AGE:7}") int maxIndexAge,
							 @Value("${ELASTICSEARCH_INDEX_MAX_GB_SIZE:100}") int maxIndexSizeInGB,
							 @Value("${ELASTICSEARCH_INDEX_MAX_DOCS:1000000000}") int maxIndexDocs,
							 @Value("${ELASTICSEARCH_MAX_TOTAL_FIELDS:4000}") int maxTotalFields,
							 @Value("${ELASTICSEARCH_MAX_SEARCH_SIZE:1000}") int searchMaxSize,
							 @Value("${ELASTICSEARCH_ACTION_TRIES:3}") int numOfElasticSearchActionsTries,
							 @Value("${INDEXING_THREADS:10}") int indexingThreads,
							 @Value("${DAYS_ROTATION:90}") Integer daysRotation,
							 @Value("${TIMBERMILL_VERSION:}") String timbermillVersion,
							 @Value("${TERMINATION_TIMEOUT_SECONDS:60}") int terminationTimeoutSeconds,
							 @Value("${PLUGINS_JSON:[]}") String pluginsJson,
							 @Value("${EVENT_MAX_ELEMTS:100000}") int eventsMaxElement,
							 @Value("${EVENT_QUEUE_CAPACITY:10000000}") int eventsQueueCapacity,
							 @Value("${OVERFLOWED_QUEUE_CAPACITY:10000000}") int overFlowedQueueCapacity,
							 @Value("${MAX_BULK_INDEX_FETCHES:3}") int maxBulkIndexFetches,
							 @Value("${MERGING_CRON_EXPRESSION:0 0/10 * 1/1 * ? *}") String mergingCronExp,
							 @Value("${DELETION_CRON_EXPRESSION:0 0 12 1/1 * ? *}") String deletionCronExp,
							 @Value("${DELETION_CRON_MAX_INDICES_IN_PARALLEL:1}") int expiredMaxIndicesToDeleteInParallel,
							 @Value("${PERSISTENCE_STRATEGY:sqlite}") String persistenceStrategy,
							 @Value("${BULK_PERSISTENT_FETCH_CRON_EXPRESSION:0 0/1 * 1/1 * ? *}") String bulkPersistentFetchCronExp,
							 @Value("${EVENTS_PERSISTENT_FETCH_CRON_EXPRESSION:0 0/5 * 1/1 * ? *}") String eventsPersistentFetchCronExp,
							 @Value("${MAX_FETCHED_BULKS_IN_ONE_TIME:100}") int maxFetchedBulksInOneTime,
							 @Value("${MAX_FETCHED_EVENTS_IN_ONE_TIME:10}") int maxOverflowedEventsInOneTime,
							 @Value("${MAX_INSERT_TRIES:3}") int maxInsertTries,
							 @Value("${LOCATION_IN_DISK:/db}") String locationInDisk,
							 @Value("${PERSISTENCE_TTL_IN_SECONDS:86400}") int persistenceRedisTtlInSec,
							 @Value("${SCROLL_LIMITATION:1000}") int scrollLimitation,
							 @Value("${SCROLL_TIMEOUT_SECONDS:60}") int scrollTimeoutSeconds,
							 @Value("${MAXIMUM_TASKS_CACHE_WEIGHT:100000000}") long maximumTasksCacheWeight,
							 @Value("${MAXIMUM_ORPHANS_CACHE_WEIGHT:1000000000}") long maximumOrphansCacheWeight,
							 @Value("${CACHE_STRATEGY:}") String cacheStrategy,
							 @Value("${CACHE_TTL_IN_SECONDS:604800}") int cacheRedisTtlInSeconds,
							 @Value("${REDIS_MAX_MEMORY:}") String redisMaxMemory,
							 @Value("${REDIS_MAX_MEMORY_POLICY:}") String redisMaxMemoryPolicy,
							 @Value("${REDIS_HOST:}") String redisHost,
							 @Value("${REDIS_PORT:6379}") int redisPort,
							 @Value("${REDIS_PASS:}") String redisPass,
							 @Value("${REDIS_USE_SSL:false}") Boolean redisUseSsl,
							 @Value("${REDIS_GET_SIZE:100}") int redisGetSize,
							 @Value("${REDIS_POOL_MIN_IDLE:50}") int redisPoolMinIdle,
							 @Value("${REDIS_POOL_MAX_IDLE:50}") int redisPoolMaxIdle,
							 @Value("${REDIS_POOL_MAX_TOTAL:50}") int redisPoolMaxTotal,
							 @Value("${REDIS_MAX_TRIED:3}") int redisMaxTries,
							 @Value("${FETCH_BY_IDS_PARTITIONS:10000}") int fetchByIdsPartitions,
                             @Value("${LIMIT_FOR_PERIOD:30000}") int limitForPeriod,
                             @Value("${LIMIT_REFRESH_PERIOD_MINUTES:1}") int limitRefreshPeriod,
							 @Value("${RATE_LIMITER_CAPACITY:1000000}") int rateLimiterCapacity) {

		eventsQueue = new LinkedBlockingQueue<>(eventsQueueCapacity);
		overflowedQueue = new LinkedBlockingQueue<>(overFlowedQueueCapacity);
		terminationTimeout = terminationTimeoutSeconds * 1000;

		RedisService redisService = null;
		if (!StringUtils.isEmpty(redisHost)) {
			redisService = new RedisService(redisHost, redisPort, redisPass, redisMaxMemory,
					redisMaxMemoryPolicy, redisUseSsl, redisGetSize, redisPoolMinIdle, redisPoolMaxIdle,
					redisPoolMaxTotal, redisMaxTries);
		}
		Map<String, Object> params = PersistenceHandler.buildPersistenceHandlerParams(maxFetchedBulksInOneTime, maxOverflowedEventsInOneTime, maxInsertTries, locationInDisk, persistenceRedisTtlInSec, redisService);
		persistenceHandler = PersistenceHandlerUtil.getPersistenceHandler(persistenceStrategy, params);
        rateLimiterMap = RateLimiterUtil.initRateLimiter(limitForPeriod, Duration.ofMinutes(limitRefreshPeriod), rateLimiterCapacity);


		ElasticsearchClient es = new ElasticsearchClient(elasticUrl, indexBulkSize, indexingThreads, awsRegion, elasticUser,
				elasticPassword, maxIndexAge, maxIndexSizeInGB, maxIndexDocs, numOfElasticSearchActionsTries, maxBulkIndexFetches, searchMaxSize, persistenceHandler, numberOfShards, numberOfReplicas,
				maxTotalFields, null, scrollLimitation, scrollTimeoutSeconds, fetchByIdsPartitions, expiredMaxIndicesToDeleteInParallel);

		CacheConfig cacheParams = new CacheConfig(redisService, cacheRedisTtlInSeconds, maximumTasksCacheWeight, maximumOrphansCacheWeight);
		AbstractCacheHandler cacheHandler = CacheHandlerUtil.getCacheHandler(cacheStrategy, cacheParams);
		this.eventsMaxElement = eventsMaxElement;
		taskIndexer = new TaskIndexer(pluginsJson, daysRotation, es, timbermillVersion, cacheHandler);
		cronsRunner.runCrons(bulkPersistentFetchCronExp, eventsPersistentFetchCronExp, persistenceHandler, es, deletionCronExp,
				eventsQueue, overflowedQueue, mergingCronExp, redisService, rateLimiterMap);
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
				ElasticsearchUtil.drainAndIndex(eventsQueue, taskIndexer, eventsMaxElement);
			}
			stoppedRunning = true;
		});
		workingThread.start();
	}

	@PreDestroy
	public void tearDown(){
		LOG.info("Gracefully shutting down Timbermill Server.");
		keepRunning = false;
		long currentTimeMillis = System.currentTimeMillis();
		while(!stoppedRunning && !reachTerminationTimeout(currentTimeMillis)){
			try {
				Thread.sleep(ElasticsearchUtil.THREAD_SLEEP);
			} catch (InterruptedException ignored) {}
		}
		if (persistenceHandler != null){
			persistenceHandler.close();
		}
		taskIndexer.close();
		cronsRunner.close();
		LOG.info("Timbermill server was shut down.");
	}

	private boolean reachTerminationTimeout(long starTime) {
		boolean reachTerminationTimeout = System.currentTimeMillis() - starTime > terminationTimeout;
		if (reachTerminationTimeout){
			LOG.warn("Timbermill couldn't gracefully shutdown in {} seconds, was killed with {} events in internal buffer", terminationTimeout / 1000, eventsQueue.size());
		}
		return reachTerminationTimeout;
	}

	void handleEvents(Collection<Event> events){
		for (Event event : events) {
			event.setTaskId(getTaskIdContextPair(event.getTaskId()).getLeft());
			LocalOutputPipe.pushEventToQueues(persistenceHandler, eventsQueue, overflowedQueue, rateLimiterMap, event);
		}
	}

	PersistenceHandler getPersistenceHandler() {
		return persistenceHandler;
	}

	private ImmutablePair<String, Object> getTaskIdContextPair(String pairStr) {
		LOG.info("trying to parse taskId {}", pairStr);
		try {
			return gson.fromJson(pairStr, pairType);
		} catch (Exception e) {
			return ImmutablePair.of(pairStr, null);
		}
	}
}

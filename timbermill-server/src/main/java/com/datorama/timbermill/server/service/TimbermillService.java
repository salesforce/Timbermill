package com.datorama.timbermill.server.service;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.datorama.oss.timbermill.ElasticsearchClient;
import com.datorama.oss.timbermill.TaskIndexer;
import com.datorama.oss.timbermill.common.ElasticsearchUtil;
import com.datorama.oss.timbermill.common.disk.DiskHandler;
import com.datorama.oss.timbermill.common.disk.DiskHandlerUtil;
import com.datorama.oss.timbermill.cron.CronsRunner;
import com.datorama.oss.timbermill.unit.Event;

@Service
public class TimbermillService {

	private static final Logger LOG = LoggerFactory.getLogger(TimbermillService.class);
	private static final int THREAD_SLEEP = 2000;
	private final String mergingCronExp;
	private TaskIndexer taskIndexer;
	private BlockingQueue<Event> eventsQueue;
	private BlockingQueue<Event> overflowedQueue;

	private boolean keepRunning = true;
	private boolean stoppedRunning = false;
	private long terminationTimeout;
	private DiskHandler diskHandler;
	private CronsRunner cronsRunner;

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
			@Value("${TERMINATION_TIMEOUT_SECONDS:60}") int terminationTimeoutSeconds,
			@Value("${PLUGINS_JSON:[]}") String pluginsJson,
			@Value("${EVENT_QUEUE_CAPACITY:10000000}") int eventsQueueCapacity,
			@Value("${OVERFLOWED_QUEUE_CAPACITY:10000000}") int overFlowedQueueCapacity,
			@Value("${MAX_BULK_INDEX_FETCHES:3}") int maxBulkIndexFetches,
			@Value("${MERGING_CRON_EXPRESSION:0 0 0/1 1/1 * ? *}") String mergingCronExp,
			@Value("${DELETION_CRON_EXPRESSION:0 0 12 1/1 * ? *}") String deletionCronExp,
			@Value("${DISK_HANDLER_STRATEGY:sqlite}") String diskHandlerStrategy,
			@Value("${BULK_PERSISTENT_FETCH_CRON_EXPRESSION:0 0/10 * 1/1 * ? *}") String bulkPersistentFetchCronExp,
			@Value("${EVENTS_PERSISTENT_FETCH_CRON_EXPRESSION:0 0/5 * 1/1 * ? *}") String eventsPersistentFetchCronExp,
			@Value("${MAX_FETCHED_BULKS_IN_ONE_TIME:100}") int maxFetchedBulksInOneTime,
			@Value("${MAX_INSERT_TRIES:10}") int maxInsertTries,
			@Value("${LOCATION_IN_DISK:/db}") String locationInDisk,
			@Value("${ORPHANS_ADOPTION_CRON_EXPRESSION:0 */10 * ? * *}") String orphansAdoptionsCronExp,
			@Value("${ORPHANS_FETCH_PERIOD_MINUTES:10}") int orphansFetchPeriodMinutes ){

		eventsQueue = new LinkedBlockingQueue<>(eventsQueueCapacity);
		overflowedQueue = new LinkedBlockingQueue<>(overFlowedQueueCapacity);
		terminationTimeout = terminationTimeoutSeconds * 1000;
		this.mergingCronExp = mergingCronExp;

		Map<String, Object> params = DiskHandler.buildDiskHandlerParams(maxFetchedBulksInOneTime, maxInsertTries, locationInDisk);
		diskHandler = DiskHandlerUtil.getDiskHandler(diskHandlerStrategy, params);
		ElasticsearchClient es = new ElasticsearchClient(elasticUrl, indexBulkSize, indexingThreads, awsRegion, elasticUser,
				elasticPassword, maxIndexAge, maxIndexSizeInGB, maxIndexDocs, numOfElasticSearchActionsTries, maxBulkIndexFetches, searchMaxSize, diskHandler, numberOfShards, numberOfReplicas,
				maxTotalFields, null);

		taskIndexer = new TaskIndexer(pluginsJson, maximumCacheSize, maximumCacheMinutesHold, daysRotation, es);
		cronsRunner = new CronsRunner();
		cronsRunner.runCrons(bulkPersistentFetchCronExp, eventsPersistentFetchCronExp, diskHandler, es, deletionCronExp, eventsQueue, overflowedQueue);

		startWorkingThread();
	}

	private void startWorkingThread() {
		Runnable eventsHandler = () -> {
			LOG.info("Timbermill has started");
			while (keepRunning) {
				ElasticsearchUtil.drainAndIndex(eventsQueue, overflowedQueue, taskIndexer, mergingCronExp, diskHandler);
			}
			stoppedRunning = true;
		};

		Thread workingThread = new Thread(eventsHandler);
		workingThread.start();
	}

	@PreDestroy
	public void tearDown(){
		LOG.info("Gracefully shutting down Timbermill Server.");
		keepRunning = false;
		long currentTimeMillis = System.currentTimeMillis();
		while(!stoppedRunning && !reachTerminationTimeout(currentTimeMillis)){
			try {
				Thread.sleep(THREAD_SLEEP);
			} catch (InterruptedException ignored) {}
		}
		if (diskHandler != null){
			diskHandler.close();
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
			if(!this.eventsQueue.offer(event)){
				if (!overflowedQueue.offer(event)){
					LOG.error("OverflowedQueue is full, event {} was discarded", event.getTaskId());
				}
			}
		}
	}

	int getEventsQueueSize() {
		return eventsQueue.size();
	}

	TaskIndexer getTaskIndexer() {
		return taskIndexer;
	}
}

package com.datorama.timbermill.server.service;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.datorama.timbermill.ElasticsearchClient;
import com.datorama.timbermill.ElasticsearchParams;
import com.datorama.timbermill.TaskIndexer;
import com.datorama.timbermill.common.TimbermillUtils;
import com.datorama.timbermill.unit.Event;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class TimbermillService {

	private static final Logger LOG = LoggerFactory.getLogger(TimbermillService.class);
	private static final int THREAD_SLEEP = 2000;
	private TaskIndexer taskIndexer;
	private static final int EVENT_QUEUE_CAPACITY = 10000000;
	private final BlockingQueue<Event> eventsQueue = new LinkedBlockingQueue<>(EVENT_QUEUE_CAPACITY);

	private boolean keepRunning = true;
	private boolean stoppedRunning = false;
	private long terminationTimeout;

	@Autowired
	public TimbermillService(@Value("${INDEX_BULK_SIZE:200000}") Integer indexBulkSize,
							 @Value("${ELASTICSEARCH_URL:http://localhost:9200}") String elasticUrl,
							 @Value("${ELASTICSEARCH_AWS_REGION:}") String awsRegion,
							 @Value("${days.rotation:90}") Integer daysRotation,
							 @Value("${plugins.json:[]}") String pluginsJson,
							 @Value("${properties.length.json:{}}") String propertiesLengthJson,
							 @Value("${termination.timeout.seconds:60}") int terminationTimeoutSeconds,
							 @Value("${indexing.threads:10}") int indexingThreads,
							 @Value("${elasticsearch.user:}") String elasticUser,
							 @Value("${elasticsearch.password:}") String elasticPassword,
							 @Value("${cache.max.size:10000}") int maximumCacheSize,
							 @Value("${elasticsearch.number.of.shards:7}") int numberOfShards,
							 @Value("${elasticsearch.number.of.replicas:1}") int numberOfReplicas,
							 @Value("${number.of.tasks.merge.retries:3}") int numOfMergedTasksTries,
							 @Value("${number.of.tasks.index.retries:3}") int numOfTasksIndexTries,
							 @Value("${elasticsearch.index.max.age:7}") long maxIndexAge,
							 @Value("${elasticsearch.index.max.gb.size:100}") long maxIndexSizeInGB,
							 @Value("${elasticsearch.index.max.docs:1000000000}") long maxIndexDocs,
							 @Value("${deletion.cron.expression:0 0 12 1/1 * ? *}") String deletionCronExp,
							 @Value("${merging.cron.expression:0 0 0/1 1/1 * ? *}") String mergingCronExp,
							 @Value("${cache.max.hold.time.minutes:6}") int maximumCacheMinutesHold) throws IOException {

		terminationTimeout = terminationTimeoutSeconds * 1000;
		Map propertiesLengthJsonMap = new ObjectMapper().readValue(propertiesLengthJson, Map.class);
		ElasticsearchParams elasticsearchParams = new ElasticsearchParams(
				pluginsJson, propertiesLengthJsonMap, maximumCacheSize, maximumCacheMinutesHold, numberOfShards, numberOfReplicas, daysRotation, deletionCronExp, mergingCronExp);
		ElasticsearchClient es = new ElasticsearchClient(elasticUrl, indexBulkSize, indexingThreads, awsRegion, elasticUser, elasticPassword, maxIndexAge, maxIndexSizeInGB, maxIndexDocs,
				numOfMergedTasksTries, numOfTasksIndexTries);
		taskIndexer = TimbermillUtils.bootstrap(elasticsearchParams, es);
		startWorkingThread(es);
	}

	private void startWorkingThread(ElasticsearchClient es) {
		Runnable eventsHandler = () -> {
			LOG.info("Timbermill has started");
			while (keepRunning) {
				TimbermillUtils.drainAndIndex(eventsQueue, taskIndexer, es);
			}
			stoppedRunning = true;
		};

		Thread workingThread = new Thread(eventsHandler);
		workingThread.start();
	}

	public void tearDown(){
		LOG.info("Gracefully shutting down Timbermill Server.");
		keepRunning = false;
		long currentTimeMillis = System.currentTimeMillis();
		while(!stoppedRunning && !reachTerminationTimeout(currentTimeMillis)){
			try {
				Thread.sleep(THREAD_SLEEP);
			} catch (InterruptedException ignored) {}
		}
		taskIndexer.close();
		LOG.info("Timbermill server was shut down.");
	}

	private boolean reachTerminationTimeout(long starTime) {
		boolean reachTerminationTimeout = System.currentTimeMillis() - starTime > terminationTimeout;
		if (reachTerminationTimeout){
			LOG.warn("Timbermill couldn't gracefully shutdown in {} seconds, was killed with {} events in internal buffer", terminationTimeout / 1000, eventsQueue.size());
		}
		return reachTerminationTimeout;
	}

	public void handleEvent(Collection<Event> events){
		for (Event event : events) {
			if(!this.eventsQueue.offer(event)){
				LOG.warn("Event {} was removed from the queue due to insufficient space", event.getTaskId());
			}
		}
	}
}

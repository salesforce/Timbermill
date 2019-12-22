package com.datorama.timbermill.server.service;

import com.datorama.timbermill.ElasticsearchParams;
import com.datorama.timbermill.TaskIndexer;
import com.datorama.timbermill.unit.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;


@Service
public class TimbermillService {

	private static final Logger LOG = LoggerFactory.getLogger(TimbermillService.class);
	private static final int THREAD_SLEEP = 2000;
	private final TaskIndexer taskIndexer;
	private static final int EVENT_QUEUE_CAPACITY = 10000000;
	private final BlockingQueue<Event> eventsQueue = new ArrayBlockingQueue<>(EVENT_QUEUE_CAPACITY);

	private boolean keepRunning = true;
	private boolean stoppedRunning = false;
	private long terminationTimeout;

	@Autowired
	public TimbermillService(@Value("${index.bulk.size:2097152}") Integer indexBulkSize,
							 @Value("${elasticsearch.url:http://localhost:9200}") String elasticUrl,
							 @Value("${elasticsearch.aws.region:}") String awsRegion,
							 @Value("${days.rotation:90}") Integer daysRotation,
							 @Value("${plugins.json:[]}") String pluginsJson,
							 @Value("${properties.length.json:{}}") String propertiesLengthJson,
							 @Value("${default.max.chars:100000}") int defaultMaxChars,
							 @Value("${termination.timeout.seconds:60}") int terminationTimeoutSeconds,
							 @Value("${indexing.threads:1}") int indexingThreads,
							 @Value("${elasticsearch.user:}") String elasticUser,
							 @Value("${elasticsearch.password:}") String elasticPassword,
							 @Value("${cache.max.size:10000}") int maximumCacheSize,
							 @Value("${cache.max.hold.time.minutes:6}") int maximumCacheMinutesHold) throws IOException {

		terminationTimeout = terminationTimeoutSeconds * 1000;
		Map propertiesLengthJsonMap = new ObjectMapper().readValue(propertiesLengthJson, Map.class);
		ElasticsearchParams elasticsearchParams = new ElasticsearchParams(elasticUrl, defaultMaxChars, daysRotation, awsRegion, indexingThreads, elasticUser, elasticPassword, indexBulkSize,
				pluginsJson, propertiesLengthJsonMap, maximumCacheSize, maximumCacheMinutesHold);
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
		eventsQueue.addAll(events);
	}
}

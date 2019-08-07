package com.datorama.timbermill.server.service;

import com.datorama.timbermill.TaskIndexer;
import com.datorama.timbermill.unit.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


@Service
public class TimbermillService {

	private static final Logger LOG = LoggerFactory.getLogger(TimbermillService.class);
	private final TaskIndexer taskIndexer;

	private BlockingQueue<Event> eventsQueue = new ArrayBlockingQueue<>(10000000);
	private boolean keepRunning = true;
	private boolean stoppedRunning = false;

	@Autowired
	public TimbermillService(@Value("${index.bulk.size:10000}") Integer indexBulkSize,
							 @Value("${elasticsearch.url:http://localhost:9200}") String elasticUrl,
							 @Value("${elasticsearch.aws.region:}") String awsRegion,
							 @Value("${env:default}") String env,
							 @Value("${days.rotation:90}") Integer daysRotation,
							 @Value("${plugins.json:[]}") String pluginsJson,
							 @Value("${properties.length.json:{}}") String propertiesLengthJson,
							 @Value("${default.max.chars:100000}") int defaultMaxChars){

		taskIndexer = new TaskIndexer(pluginsJson, propertiesLengthJson, defaultMaxChars, env, elasticUrl, daysRotation, awsRegion, indexBulkSize);
		taskIndexer.indexStartupEvent();

		new Thread(() -> {
				while (keepRunning) {
					try {
						List<Event> events = new ArrayList<>();
						eventsQueue.drainTo(events, indexBulkSize);
						if (!events.isEmpty()) {
							taskIndexer.retrieveAndIndex(events);
						}
					} catch (RuntimeException e) {
						LOG.error("Error was thrown from TaskIndexer:", e);
					}
				}
				stoppedRunning = true;
		}).start();
	}

	@PreDestroy
	public void tearDown(){
		LOG.info("Gracefully shutting down Timbermill Server.");
		keepRunning = false;
		while(!stoppedRunning){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException ignored) {}
		}
		taskIndexer.close();
		LOG.info("Timbermill Server is shut down.");
	}

	public void handleEvent(Event event){
		eventsQueue.add(event);
	}
}

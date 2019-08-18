package com.datorama.timbermill.server.service;

import com.datorama.timbermill.TaskIndexer;
import com.datorama.timbermill.unit.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;


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
							 @Value("${days.rotation:90}") Integer daysRotation,
							 @Value("${plugins.json:[]}") String pluginsJson,
							 @Value("${properties.length.json:{}}") String propertiesLengthJson,
							 @Value("${default.max.chars:100000}") int defaultMaxChars, @Value("${index.bulk.sleep.cycle:1000}") long indexBulkSleepCycle) throws IOException {

		Map propertiesLengthJsonMap = new ObjectMapper().readValue(propertiesLengthJson, Map.class);
		taskIndexer = new TaskIndexer(pluginsJson, propertiesLengthJsonMap, defaultMaxChars, elasticUrl, daysRotation, awsRegion, indexBulkSize);

		new Thread(() -> {
				while (keepRunning) {
					try {
						List<Event> events = new ArrayList<>();
                        eventsQueue.drainTo(events);
                        Map<String, List<Event>> eventsPerEnvMap = events.stream().collect(Collectors.groupingBy(event -> event.getEnv()));
                        for (Map.Entry<String, List<Event>> eventsPerEnv : eventsPerEnvMap.entrySet()) {
                            String env = eventsPerEnv.getKey();
                            List<Event> currentEvents = eventsPerEnv.getValue();
                            taskIndexer.retrieveAndIndex(currentEvents, env);
                        }
						Thread.sleep(indexBulkSleepCycle);
					} catch (RuntimeException | InterruptedException e) {
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

	public void handleEvent(List<Event> events){
		eventsQueue.addAll(events);
	}
}

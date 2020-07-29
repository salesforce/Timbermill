package com.datorama.timbermill.server.service;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import kamon.Kamon;
import kamon.metric.Metric;

/**
 * this is an optional reporting service that reports various metrics using kamon to datadog
 * it is off by default.
 * current reported metrics are:
 *   - input queue size
 */
@Service
@ConditionalOnProperty(name = "KAMON.MONITORING.ENABLED", matchIfMissing = false, havingValue = "true")
public class KamonMonitorService {

	private static final Logger LOG = LoggerFactory.getLogger(KamonMonitorService.class);
	private static final long MONITOR_RATE_IN_MS = 20000;
	private static final long MONITOR_DELAY_IN_MS = 20000;
	private final Metric.Histogram totalMessagesInInputQueueHistogram = Kamon.histogram("timbermill2.inputQueue.size.histogram");
	private TimbermillService timbermillService;

	@Autowired
	public KamonMonitorService(TimbermillService timbermillService) {
		this.timbermillService = timbermillService;
	}

	@PostConstruct void init(){
		LOG.info("Init KamonMonitorService, will report data every {} mili-seconds",MONITOR_RATE_IN_MS);
		Kamon.init();
	}

	@Scheduled(fixedRate = MONITOR_RATE_IN_MS,initialDelay = MONITOR_DELAY_IN_MS)
	private void runMonitor() {
		LOG.trace("KamonMonitorService running");
		reportInputQueueParams();
		LOG.trace("KamonMonitorService finished");

	}

	private void reportInputQueueParams() {
		if (timbermillService!=  null){
			totalMessagesInInputQueueHistogram.withoutTags().record(timbermillService.getEventsQueueSize());
		}
	}
}

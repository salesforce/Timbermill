package com.datorama.timbermill.server.service;

import java.util.Queue;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.datorama.oss.timbermill.unit.AdoptedEvent;
import com.google.common.cache.Cache;

import kamon.Kamon;
import kamon.metric.Metric;

/**
 * this is an optional reporting service that reports various metrics using kamon to datadog
 * it is off by default.
 * current reported metrics are:
 *   - input queue size
 *   - orphan stats - size / eviction count / hit count
 */
@Service
@ConditionalOnProperty(name = "KAMON.MONITORING.ENABLED", matchIfMissing = false, havingValue = "true")
public class KamonMonitorService {

	private static final Logger LOG = LoggerFactory.getLogger(KamonMonitorService.class);
	private static final long MONITOR_RATE_IN_MS = 20000;
	private static final long MONITOR_DELAY_IN_MS = 20000;
	private Metric.Histogram totalMessagesInInputQueueHistogram = Kamon.histogram("timbermill2.inputQueue.size.histogram");
	private Metric.Histogram orphanMapSizeHistogram = Kamon.histogram("timbermill2.orphanMap.size.histogram");
	private Metric.Histogram orphanEvictionCountHistogram = Kamon.histogram("timbermill2.orphanMap.evictionCount.histogram");
	private Metric.Histogram orphanHitsCountHistogram = Kamon.histogram("timbermill2.orphanMap.hits.histogram");
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
		reportOrphanMapStats();
		LOG.trace("KamonMonitorService finished");

	}

	private void reportOrphanMapStats() {
		if (timbermillService!=  null){
			final Cache<String, Queue<AdoptedEvent>> orphansEventsCache = timbermillService.getTaskIndexer().getParentIdTORootOrphansEventsCache();
			orphanMapSizeHistogram.withoutTags().record(orphansEventsCache.size());
			orphanEvictionCountHistogram.withoutTags().record(orphansEventsCache.stats().evictionCount());
			orphanHitsCountHistogram.withoutTags().record(orphansEventsCache.stats().hitCount());
		}
	}

	private void reportInputQueueParams() {
		if (timbermillService!=  null){
			totalMessagesInInputQueueHistogram.withoutTags().record(timbermillService.getEventsQueueSize());
		}
	}
}

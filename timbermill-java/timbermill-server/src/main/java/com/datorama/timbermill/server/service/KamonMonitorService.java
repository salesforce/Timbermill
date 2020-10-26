package com.datorama.timbermill.server.service;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import kamon.Kamon;

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

	@Autowired
	public KamonMonitorService() {
	}

	@PostConstruct void init(){
		LOG.info("Init KamonMonitorService, will report data every {} mili-seconds",MONITOR_RATE_IN_MS);
		Kamon.init();
	}
}

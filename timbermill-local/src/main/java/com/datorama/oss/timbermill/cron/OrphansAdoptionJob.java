package com.datorama.oss.timbermill.cron;

import java.util.concurrent.atomic.AtomicBoolean;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.oss.timbermill.ElasticsearchClient;
import com.datorama.oss.timbermill.TaskIndexer;
import com.datorama.oss.timbermill.common.ElasticsearchUtil;

public class OrphansAdoptionJob implements Job {
	private static final Logger LOG = LoggerFactory.getLogger(OrphansAdoptionJob.class);
	private static final AtomicBoolean currentlyRunning = new AtomicBoolean(false);

	@Override public void execute(JobExecutionContext context) {

		if (currentlyRunning.compareAndSet(false, true)) {
			LOG.info("OrphansAdoptionJob started...");
			ElasticsearchClient es = (ElasticsearchClient) context.getJobDetail().getJobDataMap().get(ElasticsearchUtil.CLIENT);
			int orphansFetchPeriodMinutes = context.getJobDetail().getJobDataMap().getInt("orphansFetchPeriodMinutes");
			int daysRotationParam = context.getJobDetail().getJobDataMap().getInt("days_rotation");
			TaskIndexer.handleAdoptions(es, orphansFetchPeriodMinutes, daysRotationParam);
			LOG.info("OrphansAdoptionJob ended...");
			currentlyRunning.set(false);
		}
	}
}

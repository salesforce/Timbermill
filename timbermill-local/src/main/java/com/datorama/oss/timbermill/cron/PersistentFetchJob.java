package com.datorama.oss.timbermill.cron;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.oss.timbermill.ElasticsearchClient;
import com.datorama.oss.timbermill.common.ElasticsearchUtil;

public class PersistentFetchJob implements Job {

	private static final Logger LOG = LoggerFactory.getLogger(PersistentFetchJob.class);
	private static boolean currentlyRunning = false;

	@Override public void execute(JobExecutionContext context) {
		if (!currentlyRunning) {
			currentlyRunning = true;
			LOG.info("Cron is fetching from disk...");
			ElasticsearchClient es = (ElasticsearchClient) context.getJobDetail().getJobDataMap().get(ElasticsearchUtil.ELASTIC_SEARCH_CLIENT);
			boolean runNextBulk = true;
			while (runNextBulk) {
				runNextBulk = es.retryFailedRequestsFromDisk();
			}
			currentlyRunning = false;
		}
	}
}

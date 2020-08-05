package com.datorama.oss.timbermill.cron;

import java.time.ZonedDateTime;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.oss.timbermill.ElasticsearchClient;
import com.datorama.oss.timbermill.common.ElasticsearchUtil;

import kamon.Kamon;
import kamon.metric.Metric;
import kamon.metric.Timer;


public class TasksMergerJobs implements Job {

	private static final Logger LOG = LoggerFactory.getLogger(TasksMergerJobs.class);

	private final Metric.Timer partialsJobLatency = Kamon.timer("timbermill2.partial.tasks.job.latency.timer");

	@Override public void execute(JobExecutionContext context) {

		JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
		ElasticsearchClient client = (ElasticsearchClient) jobDataMap.get(ElasticsearchUtil.CLIENT);
		String env = jobDataMap.getString(ElasticsearchUtil.ENVIRONMENT);
		int partialsFetchPeriod = jobDataMap.getInt(ElasticsearchUtil.PARTIAL_TASKS_FETCH_PERIOD_HOURS);
		if (client.doesIndexAlreadyRolledOver()){
			Timer.Started started = partialsJobLatency.withTag(ElasticsearchUtil.ENVIRONMENT, env).start();
			LOG.info("About to merge partial tasks between indices");
			int size = client.migrateTasksToNewIndex(env, ZonedDateTime.now().minusHours(partialsFetchPeriod));
			LOG.info("Finished merging {} partial tasks.", size);
			started.stop();
		}
	}

}

package com.datorama.oss.timbermill.cron;

import java.util.Random;

import org.quartz.DisallowConcurrentExecution;
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

@DisallowConcurrentExecution
public class TasksMergerJobs implements Job {

	private static final Logger LOG = LoggerFactory.getLogger(TasksMergerJobs.class);

	private final Metric.Timer partialsJobLatency = Kamon.timer("timbermill2.partial.tasks.job.latency.timer");
	private final Random rand = new Random();

	@Override public void execute(JobExecutionContext context) {

		JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
		ElasticsearchClient client = (ElasticsearchClient) jobDataMap.get(ElasticsearchUtil.CLIENT);
		int partialsFetchPeriodMinutes = jobDataMap.getInt(ElasticsearchUtil.PARTIAL_TASKS_FETCH_PERIOD_MINUTES);
		if (client.doesIndexAlreadyRolledOver()){
			int secondsToWait = rand.nextInt(10);
			try {
				Thread.sleep(secondsToWait * 1000);
			} catch (InterruptedException ignored) {}
			Timer.Started started = partialsJobLatency.withoutTags().start();
			LOG.info("About to merge partial tasks between indices");
			int size = client.migrateTasksToNewIndex(partialsFetchPeriodMinutes);
			LOG.info("Finished merging {} partial tasks.", size);
			started.stop();
		}
	}

}

package com.datorama.oss.timbermill.cron;

import java.util.Random;
import java.util.UUID;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;

import com.datorama.oss.timbermill.ElasticsearchClient;
import com.datorama.oss.timbermill.common.ElasticsearchUtil;

@DisallowConcurrentExecution
public class ExpiredTasksDeletionJob implements Job {

	@Override public void execute(JobExecutionContext context) {
		long timeToSleep = new Random().nextInt(43200000);

		try {
			Thread.sleep(timeToSleep);
		} catch (InterruptedException ignored) {
		}

		ElasticsearchClient client = (ElasticsearchClient) context.getJobDetail().getJobDataMap().get(ElasticsearchUtil.CLIENT);
		String flowId = "Expired Tasks Deletion Job - " + UUID.randomUUID().toString();
		client.deleteExpiredTasks(flowId);
	}
}

package com.datorama.timbermill.cron;

import org.quartz.Job;
import org.quartz.JobExecutionContext;

import com.datorama.timbermill.ElasticsearchClient;
import com.datorama.timbermill.common.TimbermillUtils;

public class ExpiredTasksDeletionJob implements Job {

	@Override public void execute(JobExecutionContext context) {
		ElasticsearchClient client = (ElasticsearchClient) context.getJobDetail().getJobDataMap().get(TimbermillUtils.CLIENT);
		client.deleteExpiredTasks();
	}
}

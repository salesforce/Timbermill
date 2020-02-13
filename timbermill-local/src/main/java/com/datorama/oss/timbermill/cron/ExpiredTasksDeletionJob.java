package com.datorama.oss.timbermill.cron;

import org.quartz.Job;
import org.quartz.JobExecutionContext;

import com.datorama.oss.timbermill.ElasticsearchClient;
import com.datorama.oss.timbermill.common.ElasticsearchUtil;

public class ExpiredTasksDeletionJob implements Job {

	@Override public void execute(JobExecutionContext context) {
		ElasticsearchClient client = (ElasticsearchClient) context.getJobDetail().getJobDataMap().get(ElasticsearchUtil.CLIENT);
		client.deleteExpiredTasks();
	}
}

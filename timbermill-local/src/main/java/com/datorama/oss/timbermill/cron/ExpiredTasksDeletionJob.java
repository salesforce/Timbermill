package com.datorama.oss.timbermill.cron;

import java.util.UUID;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;

import com.datorama.oss.timbermill.ElasticsearchClient;
import com.datorama.oss.timbermill.common.ElasticsearchUtil;

@DisallowConcurrentExecution
public class ExpiredTasksDeletionJob implements Job {

	@Override public void execute(JobExecutionContext context) {
		ElasticsearchClient client = (ElasticsearchClient) context.getJobDetail().getJobDataMap().get(ElasticsearchUtil.CLIENT);
		String flowId = UUID.randomUUID().toString();
		client.deleteExpiredTasks(flowId);
	}
}

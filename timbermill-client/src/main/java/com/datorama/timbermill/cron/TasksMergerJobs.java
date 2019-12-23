package com.datorama.timbermill.cron;

import java.io.IOException;

import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.timbermill.ElasticsearchClient;
import com.datorama.timbermill.common.TimbermillUtils;

public class TasksMergerJobs implements Job {

	private static final Logger LOG = LoggerFactory.getLogger(TasksMergerJobs.class);

	@Override public void execute(JobExecutionContext context) {
		LOG.info("About to merge partial tasks between indices");

		RestHighLevelClient client = (RestHighLevelClient) context.getJobDetail().getJobDataMap().get(ElasticsearchClient.CLIENT);
		String env = (String) context.getJobDetail().getJobDataMap().get("env");
		String timbermillAlias = TimbermillUtils.getTimbermillIndexAlias(env);
		GetAliasesRequest getAliasesRequest = new GetAliasesRequest(timbermillAlias);
		try {
			GetAliasesResponse response = client.indices().getAlias(getAliasesRequest, RequestOptions.DEFAULT);
		} catch (IOException e) {

		}
	}
}

package com.datorama.timbermill.cron;

import java.io.IOException;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.joda.time.DateTime;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.timbermill.ElasticsearchClient;

public class ExpiredTasksDeletionJob implements Job {

	private static final Logger LOG = LoggerFactory.getLogger(ExpiredTasksDeletionJob.class);
	private static final String TTL_FIELD = "meta.dateToDelete";

	@Override public void execute(JobExecutionContext context) {
		LOG.info("About to delete expired tasks");
		QueryBuilder query = new RangeQueryBuilder(TTL_FIELD).lte(new DateTime());
		DeleteByQueryRequest request = new DeleteByQueryRequest("_all").setQuery(query);
		RestHighLevelClient client = (RestHighLevelClient) context.getJobDetail().getJobDataMap().get(ElasticsearchClient.CLIENT);
		RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
		builder.addHeader("wait_for_completion", "false");
		RequestOptions options = builder.build();
		try {
			BulkByScrollResponse bulkByScrollResponse = client.deleteByQuery(request, options);
			LOG.info("Deleted {} expiredTasks.", bulkByScrollResponse.getStatus().getDeleted());
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (IllegalStateException e) {
			LOG.warn("Could not perform deletion, elasicsearch client was closed.");
		}

	}
}

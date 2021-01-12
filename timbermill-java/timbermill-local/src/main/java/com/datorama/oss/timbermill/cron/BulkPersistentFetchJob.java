package com.datorama.oss.timbermill.cron;

import java.util.List;
import java.util.UUID;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.oss.timbermill.ElasticsearchClient;
import com.datorama.oss.timbermill.common.KamonConstants;
import com.datorama.oss.timbermill.common.disk.DbBulkRequest;
import com.datorama.oss.timbermill.common.disk.DiskHandler;

import kamon.metric.Timer;
import org.slf4j.MDC;

import static com.datorama.oss.timbermill.common.ElasticsearchUtil.CLIENT;
import static com.datorama.oss.timbermill.common.ElasticsearchUtil.DISK_HANDLER;

@DisallowConcurrentExecution
public class BulkPersistentFetchJob implements Job {

	private static final Logger LOG = LoggerFactory.getLogger(BulkPersistentFetchJob.class);

	@Override public void execute(JobExecutionContext context) {
		DiskHandler diskHandler = (DiskHandler) context.getJobDetail().getJobDataMap().get(DISK_HANDLER);
		if (diskHandler != null) {
			Timer.Started start = KamonConstants.BULK_FETCH_JOB_LATENCY.withoutTags().start();
			String flowId = "Failed Bulk Persistent Fetch Job - " + UUID.randomUUID().toString();
			MDC.put("id", flowId);
			LOG.info("Failed Bulks Persistent Fetch Job started.");
			ElasticsearchClient es = (ElasticsearchClient) context.getJobDetail().getJobDataMap().get(CLIENT);
			retryFailedRequestsFromDisk(es, diskHandler);
			LOG.info("Failed Bulks Persistent Fetch Job ended.");
			start.stop();
		}
	}

	private static void retryFailedRequestsFromDisk(ElasticsearchClient es, DiskHandler diskHandler) {
		String flowId = MDC.get("id");
		while(diskHandler.hasFailedBulks()){
			LOG.info("#### Retry Failed-Requests From Disk Start ####");
			List<DbBulkRequest> failedRequestsFromDisk = diskHandler.fetchAndDeleteFailedBulks();
			int failedRequests = failedRequestsFromDisk.stream().mapToInt(DbBulkRequest::numOfActions).sum();

			int successBulks = 0;
			int bulkNum = 0;
			for (DbBulkRequest failedDbBulkRequest : failedRequestsFromDisk) {
				successBulks += es.sendDbFailedBulkRequest(failedDbBulkRequest, flowId, bulkNum);
				bulkNum++;
			}
			LOG.info("#### Retry Failed-Requests From Disk End ({}/{} fetched bulks re-processed successfully) ####", successBulks, failedRequests);
		}
	}
}

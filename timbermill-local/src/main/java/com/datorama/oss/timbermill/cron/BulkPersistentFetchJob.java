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
import static com.datorama.oss.timbermill.TaskIndexer.FLOW_ID_LOG;
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
			LOG.info(FLOW_ID_LOG + " Failed Bulks Persistent Fetch Job started.", flowId);
			ElasticsearchClient es = (ElasticsearchClient) context.getJobDetail().getJobDataMap().get(CLIENT);
			boolean runNextBulk = true;
			while (runNextBulk) {
				runNextBulk = retryFailedRequestsFromDisk(es, diskHandler, flowId);
			}
			LOG.info(FLOW_ID_LOG + " Failed Bulks Persistent Fetch Job ended.", flowId);
			start.stop();
		}
	}

	private static boolean retryFailedRequestsFromDisk(ElasticsearchClient es, DiskHandler diskHandler, String flowId) {
		boolean keepRunning = false;
		if (diskHandler.hasFailedBulks(flowId)) {
			keepRunning = true;
			int successBulks = 0;
			LOG.info(FLOW_ID_LOG + " #### Retry Failed-Requests From Disk Start ####", flowId);
			List<DbBulkRequest> failedRequestsFromDisk = diskHandler.fetchAndDeleteFailedBulks(flowId);
			if (failedRequestsFromDisk.size() == 0) {
				keepRunning = false;
			}
			int bulkNum = 1;
			for (DbBulkRequest dbBulkRequest : failedRequestsFromDisk) {
				if (es.sendDbBulkRequest(dbBulkRequest, flowId, bulkNum) > 0) {
					keepRunning = false;
				}
				else {
					successBulks += 1;
				}
				bulkNum++;
			}
			LOG.info(FLOW_ID_LOG + " #### Retry Failed-Requests From Disk End ({}/{} fetched bulks re-processed successfully) ####", flowId, successBulks,failedRequestsFromDisk.size());
		} else {
			LOG.info(FLOW_ID_LOG + " There are no failed bulks to fetch from disk.", flowId);
		}
		return keepRunning;
	}
}

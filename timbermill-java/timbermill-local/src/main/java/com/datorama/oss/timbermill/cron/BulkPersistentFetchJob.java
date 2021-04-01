package com.datorama.oss.timbermill.cron;

import com.datorama.oss.timbermill.ElasticsearchClient;
import com.datorama.oss.timbermill.common.KamonConstants;
import com.datorama.oss.timbermill.common.persistence.DbBulkRequest;
import com.datorama.oss.timbermill.common.persistence.PersistenceHandler;
import kamon.metric.Timer;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.List;
import java.util.UUID;

import static com.datorama.oss.timbermill.common.ElasticsearchUtil.CLIENT;
import static com.datorama.oss.timbermill.common.ElasticsearchUtil.PERSISTENCE_HANDLER;

@DisallowConcurrentExecution
public class BulkPersistentFetchJob implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(BulkPersistentFetchJob.class);

    @Override
    public void execute(JobExecutionContext context) {
        PersistenceHandler persistenceHandler = (PersistenceHandler) context.getJobDetail().getJobDataMap().get(PERSISTENCE_HANDLER);
        if (persistenceHandler != null) {
            KamonConstants.CURRENT_DATA_IN_DB_GAUGE.withTag("type", "failed_bulks_amount").update(persistenceHandler.failedBulksAmount());
            Timer.Started start = KamonConstants.BULK_FETCH_JOB_LATENCY.withoutTags().start();
            String flowId = "Failed Bulk Persistent Fetch Job - " + UUID.randomUUID().toString();
            MDC.put("id", flowId);
            LOG.info("Failed Bulks Persistent Fetch Job started.");
            ElasticsearchClient es = (ElasticsearchClient) context.getJobDetail().getJobDataMap().get(CLIENT);
            retryFailedRequests(es, persistenceHandler);
            LOG.info("Failed Bulks Persistent Fetch Job ended.");
            start.stop();
        }
    }

    private static void retryFailedRequests(ElasticsearchClient es, PersistenceHandler persistenceHandler) {
        String flowId = MDC.get("id");
        while (persistenceHandler.hasFailedBulks()) {
            LOG.info("#### Retry Failed-Requests Start ####");
            List<DbBulkRequest> failedRequests = persistenceHandler.fetchAndDeleteFailedBulks();
            int numOfFailedRequests = failedRequests.stream().mapToInt(DbBulkRequest::numOfActions).sum();

            int successRequests = 0;
            int bulkNum = 0;
            for (DbBulkRequest failedDbBulkRequest : failedRequests) {
                successRequests += es.sendDbFailedBulkRequest(failedDbBulkRequest, flowId, bulkNum++);
            }
            LOG.info("#### Retry Failed-Requests End ({}/{} fetched bulks re-processed successfully) ####", successRequests, numOfFailedRequests);
            if (successRequests == 0) {
                break;
            }
        }
    }
}

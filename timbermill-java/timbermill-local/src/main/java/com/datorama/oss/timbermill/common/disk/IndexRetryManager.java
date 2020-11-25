package com.datorama.oss.timbermill.common.disk;

import java.util.List;
import java.util.stream.Collectors;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.oss.timbermill.Bulker;
import com.datorama.oss.timbermill.common.KamonConstants;
import com.datorama.oss.timbermill.common.exceptions.MaximumInsertTriesException;
import com.google.common.collect.Lists;

import static com.datorama.oss.timbermill.TaskIndexer.FLOW_ID_LOG;

public class IndexRetryManager {

	private static final Logger LOG = LoggerFactory.getLogger(IndexRetryManager.class);
	private int numOfElasticSearchActionsTries;
	private DiskHandler diskHandler;
	private Bulker bulker;
	private int maxBulkIndexFetches; // after such number of fetches, bulk is considered as failed and won't be persisted anymore
	private List<String> blackListExceptions =  Lists.newArrayList("type=null_pointer_exception");

	public IndexRetryManager(int numOfElasticSearchActionsTries, int maxBulkIndexFetches, DiskHandler diskHandler, Bulker bulker) {
		this.numOfElasticSearchActionsTries = numOfElasticSearchActionsTries;
		this.diskHandler = diskHandler;
		this.bulker = bulker;
		this.maxBulkIndexFetches = maxBulkIndexFetches;
	}


	//Return failed amount of requests
	public int retrySendDbBulkRequest(DbBulkRequest dbBulkRequest, BulkResponse responses, String failureMessage, String flowId, int bulkNum){

		dbBulkRequest = extractFailedRequestsFromBulk(dbBulkRequest, responses);
		if (shouldStopRetry(failureMessage)) {
			reportStopRetry(dbBulkRequest,failureMessage);
			return dbBulkRequest.size();
		}

		for (int tryNum = 2 ; tryNum <= numOfElasticSearchActionsTries ; tryNum++) {
			LOG.info(FLOW_ID_LOG + " Bulk #{} Started bulk try # {}/{}", flowId, bulkNum, tryNum, numOfElasticSearchActionsTries);
			// continuous retries of sending the failed bulk request
			try {
				BulkResponse retryResponse = bulker.bulk(dbBulkRequest);
				if (retryResponse.hasFailures()) {
					// FAILURE
					failureMessage = retryResponse.buildFailureMessage();
					LOG.warn(FLOW_ID_LOG + " Bulk #{} Try number # {}/{} has failed, failure message: {}.",
							flowId, bulkNum, tryNum, numOfElasticSearchActionsTries, failureMessage);
					dbBulkRequest = extractFailedRequestsFromBulk(dbBulkRequest, retryResponse);
					if (shouldStopRetry(failureMessage)) {
						reportStopRetry(dbBulkRequest,failureMessage);
						return dbBulkRequest.size();
					}
				}
				else{
					// SUCCESS
					LOG.info(FLOW_ID_LOG + " Bulk #{} Try # {} finished successfully. Took: {} millis.", flowId, bulkNum, tryNum, retryResponse.getTook().millis());
					if (dbBulkRequest.getTimesFetched() > 0 ){
						KamonConstants.TASKS_FETCHED_FROM_DISK_HISTOGRAM.withTag("outcome","success").record(1);
					}
					return 0;
				}
			} catch (Throwable t) {
				// EXCEPTION
				failureMessage = t.getMessage();
				LOG.warn(FLOW_ID_LOG + " Bulk #{} Try number # {}/{} has failed, failure message: {}.",
						flowId, bulkNum, tryNum, numOfElasticSearchActionsTries, failureMessage);
				if (shouldStopRetry(failureMessage)) {
					reportStopRetry(dbBulkRequest,failureMessage);
					return dbBulkRequest.size();
				}
			}
		}
		// finishing to retry - if persistence is defined then try to persist the failed requests
		LOG.warn(FLOW_ID_LOG + " Bulk #{} Reached maximum tries ({}) attempt to index.{}", flowId, bulkNum, numOfElasticSearchActionsTries, hasPersistence()? " Bulk will be persist to disk":"");
		tryPersistBulkRequest(dbBulkRequest, flowId, bulkNum);
		return dbBulkRequest.size();
	}

	private void tryPersistBulkRequest(DbBulkRequest dbBulkRequest, String flowId, int bulkNum) {
		if (hasPersistence()) {
			if (dbBulkRequest.getTimesFetched() < maxBulkIndexFetches) {
				try {
					diskHandler.persistBulkRequestToDisk(dbBulkRequest, flowId, bulkNum);
				} catch (MaximumInsertTriesException e) {
					LOG.error(FLOW_ID_LOG + " Bulk #{} Tasks of failed bulk will not be indexed because couldn't be persisted to disk for the maximum times ({}).",
							flowId, bulkNum, e.getMaximumTriesNumber());
					KamonConstants.TASKS_FETCHED_FROM_DISK_HISTOGRAM.withTag("outcome", "error").record(1);
				}
			} else {
				LOG.error(FLOW_ID_LOG + " Bulk #{} Tasks of failed bulk {} will not be indexed because it was fetched maximum times ({}).", flowId, bulkNum, dbBulkRequest.getId(), maxBulkIndexFetches);
				KamonConstants.TASKS_FETCHED_FROM_DISK_HISTOGRAM.withTag("outcome", "failure").record(1);
			}
		}
		else {
			LOG.info(FLOW_ID_LOG + " Bulk #{} Tasks of failed bulk will not be indexed (no persistence).", flowId, bulkNum);
		}
	}

	private boolean hasPersistence() {
		return diskHandler != null;
	}

	private boolean shouldStopRetry(String failureMessage) {
		if (failureMessage != null){
			for (String ex : blackListExceptions){
				if (failureMessage.contains(ex)){
					return true;
				}
			}
		}
		return false;
	}

	private static void reportStopRetry(DbBulkRequest dbBulkRequest, String failureMessage) {
		LOG.error("Black list's exception in script. Exception {}, Requests: {}",failureMessage, dbBulkRequest.getRequest().requests().
				stream().map(DocWriteRequest::toString).collect(Collectors.joining(", ", "[", "]")));
	}

	public static DbBulkRequest extractFailedRequestsFromBulk(DbBulkRequest dbBulkRequest, BulkResponse bulkResponses) {
		if (bulkResponses != null){
			// if bulkResponses is null - an exception was thrown while bulking, then all requests failed. No change is needed in the bulk request.
			List<DocWriteRequest<?>> requests = dbBulkRequest.getRequest().requests();
			BulkItemResponse[] responses = bulkResponses.getItems();

			BulkRequest failedRequestsBulk = new BulkRequest();
			int length = requests.size();
			for (int i = 0 ; i < length; i++){
				if (responses[i].isFailed()){
					failedRequestsBulk.add(requests.get(i));
				}
			}
			dbBulkRequest = new DbBulkRequest(failedRequestsBulk).setId(dbBulkRequest.getId())
					.setTimesFetched(dbBulkRequest.getTimesFetched()).setInsertTime(dbBulkRequest.getInsertTime());
		}
		return dbBulkRequest;
	}

	public DiskHandler getDiskHandler() {
		return diskHandler;
	}

	public void setDiskHandler(DiskHandler diskHandler) {
		this.diskHandler = diskHandler;
	}
}

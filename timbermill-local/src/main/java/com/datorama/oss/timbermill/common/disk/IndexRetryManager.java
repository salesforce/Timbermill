package com.datorama.oss.timbermill.common.disk;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.oss.timbermill.Bulker;
import com.datorama.oss.timbermill.common.exceptions.MaximumInsertTriesException;
import com.google.common.collect.Lists;

import kamon.metric.Metric;

public class IndexRetryManager {

	private static final Logger LOG = LoggerFactory.getLogger(IndexRetryManager.class);
	private int numOfElasticSearchActionsTries;
	private DiskHandler diskHandler;
	private Bulker bulker;
	private int maxBulkIndexFetches; // after such number of fetches, bulk is considered as failed and won't be persisted anymore
	private Metric.Histogram tasksFetchedFromDiskCounter;
	private List<String> blackListExceptions =  Lists.newArrayList("type=null_pointer_exception");

	public IndexRetryManager(int numOfElasticSearchActionsTries, int maxBulkIndexFetches, DiskHandler diskHandler, Bulker bulker, Metric.Histogram tasksFetchedFromDiskCounter) {
		this.numOfElasticSearchActionsTries = numOfElasticSearchActionsTries;
		this.diskHandler = diskHandler;
		this.bulker = bulker;
		this.maxBulkIndexFetches = maxBulkIndexFetches;
		this.tasksFetchedFromDiskCounter = tasksFetchedFromDiskCounter;
	}

	public boolean retrySendDbBulkRequest(DbBulkRequest dbBulkRequest, BulkResponse responses, String failureMessage){
		BulkRequest request = dbBulkRequest.getRequest();
		int numberOfActions = request.numberOfActions();

		dbBulkRequest = extractFailedRequestsFromBulk(dbBulkRequest, responses);
		if (shouldStopRetry(failureMessage)) {
			reportStopRetry(dbBulkRequest,failureMessage);
			return false;
		}

		for (int retryNum = 1 ; retryNum <= numOfElasticSearchActionsTries ; retryNum++) {
			// continuous retries of sending the failed bulk request
			try {
				BulkResponse retryResponse = bulk(dbBulkRequest);
				if (retryResponse.hasFailures()) {
					// FAILURE
					failureMessage = retryResponse.buildFailureMessage();
					LOG.warn("Retry number {}/{} for requests of size {} has failed, failure message: {}.", retryNum, numOfElasticSearchActionsTries, request.estimatedSizeInBytes(), failureMessage);
					dbBulkRequest = extractFailedRequestsFromBulk(dbBulkRequest, retryResponse);
					if (shouldStopRetry(failureMessage)) {
						reportStopRetry(dbBulkRequest,failureMessage);
						return false;
					}
				}
				else{
					// SUCCESS
					LOG.debug("Batch of {} index requests finished successfully. Took: {} millis.", numberOfActions, retryResponse.getTook().millis());
					if (dbBulkRequest.getTimesFetched() > 0 ){
						tasksFetchedFromDiskCounter.withTag("outcome","success").record(1);
					}
					return true;
				}
			} catch (Throwable t) {
				// EXCEPTION
				failureMessage = t.getMessage();
				LOG.warn("Retry number {}/{} for requests of size {} has failed, failure message: {}.", retryNum, numOfElasticSearchActionsTries, request.estimatedSizeInBytes(), failureMessage);
				if (shouldStopRetry(failureMessage)) {
					reportStopRetry(dbBulkRequest,failureMessage);
					return false;
				}
			}
		}
		// finishing to retry - if persistence is defined then try to persist the failed requests
		LOG.error("Reached maximum retries ({}) attempt to index.", numOfElasticSearchActionsTries);
		tryPersistBulkRequest(dbBulkRequest);
		return false;
	}


	public void tryPersistBulkRequest(DbBulkRequest dbBulkRequest) {
		if (diskHandler != null) {
			if (dbBulkRequest.getTimesFetched() < maxBulkIndexFetches) {
				try {
					diskHandler.persistBulkRequestToDisk(dbBulkRequest);
				} catch (MaximumInsertTriesException e) {
					LOG.error("Tasks of failed bulk will not be indexed because couldn't be persisted to disk for the maximum times ({}).", e.getMaximumTriesNumber());
					tasksFetchedFromDiskCounter.withTag("outcome", "error").record(1);
				}
			} else {
				LOG.error("Tasks of failed bulk {} will not be indexed because it was fetched maximum times ({}).", dbBulkRequest.getId(), maxBulkIndexFetches);
				tasksFetchedFromDiskCounter.withTag("outcome", "failure").record(1);
			}
		}
		else {
			LOG.error("Tasks of failed bulk will not be indexed (no persistence).");
		}
	}

	BulkResponse bulk(DbBulkRequest request) throws IOException {
		return bulker.bulk(request);
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
		LOG.error("Black list's exception in script. Exception {}, Requests:",failureMessage);
		dbBulkRequest.getRequest().requests().forEach(r -> LOG.error(r.toString()));
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

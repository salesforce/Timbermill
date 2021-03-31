package com.datorama.oss.timbermill.common.persistence;

import java.util.List;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.oss.timbermill.Bulker;
import com.datorama.oss.timbermill.common.KamonConstants;
import com.google.common.collect.Lists;

public class IndexRetryManager {

	private static final Logger LOG = LoggerFactory.getLogger(IndexRetryManager.class);
	private int numOfElasticSearchActionsTries;
	private PersistenceHandler persistenceHandler;
	private Bulker bulker;
	private int maxBulkIndexFetches; // after such number of fetches, bulk is considered as failed and won't be persisted anymore
	private List<String> blackListExceptions =  Lists.newArrayList("type=null_pointer_exception", "index is missing");

	public IndexRetryManager(int numOfElasticSearchActionsTries, int maxBulkIndexFetches, PersistenceHandler persistenceHandler, Bulker bulker) {
		this.numOfElasticSearchActionsTries = numOfElasticSearchActionsTries;
		this.persistenceHandler = persistenceHandler;
		this.bulker = bulker;
		this.maxBulkIndexFetches = maxBulkIndexFetches;
	}


	//Return failed amount of requests
	public List<BulkResponse> indexBulkRequest(DbBulkRequest dbBulkRequest, int bulkNum){

		List<BulkResponse> resList = Lists.newArrayList();
		for (int tryNum = 1; tryNum <= numOfElasticSearchActionsTries ; tryNum++) {
			// continuous retries of sending the failed bulk request
			try {
				if (tryNum > 1) {
					LOG.info("Bulk #{} Started bulk try # {}/{}", bulkNum, tryNum, numOfElasticSearchActionsTries);
				}
				LOG.debug("Bulk #{} Batch of {} index requests sent to Elasticsearch. Batch size: {} bytes", bulkNum, dbBulkRequest.numOfActions(), dbBulkRequest.estimatedSize());
				BulkResponse response = bulker.bulk(dbBulkRequest);
					resList.add(response);
					if (!response.hasFailures()) {
						return successfulResponseHandling(dbBulkRequest, bulkNum, resList, tryNum, response);
					} else {
						dbBulkRequest = failureResponseHandling(dbBulkRequest, bulkNum, tryNum, response);
						if (dbBulkRequest.numOfActions() < 1) {
							LOG.info("Bulk #{} Started bulk try # {}/{} all failed response were blacklisted, no further actions will be sent.", bulkNum, tryNum, numOfElasticSearchActionsTries);
							return resList;
						}
					}
				} catch (Throwable t) {
					LOG.warn("Bulk #{} Try number #{}/{} has failed, failure message: {}.", bulkNum, tryNum, numOfElasticSearchActionsTries, t.getMessage());
				}
			}
			// finishing to retry - if persistence is defined then try to persist the failed requests
			LOG.error("Bulk #{} Reached maximum tries ({}) attempt to index.", bulkNum, numOfElasticSearchActionsTries);
			tryPersistBulkRequest(dbBulkRequest, bulkNum);
			return resList;
	}

	private DbBulkRequest failureResponseHandling(DbBulkRequest dbBulkRequest, int bulkNum, int tryNum, BulkResponse response) {
		dbBulkRequest = extractFailedRequestsFromBulk(dbBulkRequest, response);
		String failureMessage = response.buildFailureMessage();
		LOG.warn("Bulk #{} Try number #{}/{} has failed, failure message: {}.", bulkNum, tryNum, numOfElasticSearchActionsTries, failureMessage);
		return dbBulkRequest;
	}

	private List<BulkResponse> successfulResponseHandling(DbBulkRequest dbBulkRequest, int bulkNum, List<BulkResponse> resList, int tryNum, BulkResponse response) {
		if (dbBulkRequest.getTimesFetched() > 0) {
			KamonConstants.TASKS_FETCHED_FROM_DISK_HISTOGRAM.withTag("outcome", "success").record(1);
		}
		if (tryNum > 1) {
			LOG.info("Bulk #{} Try # {} finished successfully. Took: {} millis.", bulkNum, tryNum, response.getTook().millis());
		}
		else {
			LOG.debug("Bulk #{} Batch of {} index requests finished successfully. Took: {} millis.", bulkNum, dbBulkRequest.numOfActions(), response.getTook().millis());
		}
		if (dbBulkRequest.getTimesFetched() > 0 ){
			KamonConstants.TASKS_FETCHED_FROM_DISK_HISTOGRAM.withTag("outcome","success").record(1);
		}
		return resList;
	}

	private void tryPersistBulkRequest(DbBulkRequest dbBulkRequest, int bulkNum) {
		if (hasPersistence()) {
			if (dbBulkRequest.getTimesFetched() < maxBulkIndexFetches) {
				persistenceHandler.persistBulkRequest(dbBulkRequest, bulkNum);
			} else {
				LOG.error("Bulk #{} Tasks of failed bulk {} will not be indexed because it was fetched maximum times ({}).", bulkNum, dbBulkRequest.getId(), maxBulkIndexFetches);
				KamonConstants.TASKS_FETCHED_FROM_DISK_HISTOGRAM.withTag("outcome", "failure").record(1);
			}
		}
		else {
			LOG.info("Bulk #{} Tasks of failed bulk will not be indexed (no persistence).", bulkNum);
		}
	}

	private boolean hasPersistence() {
		return persistenceHandler != null;
	}

	private boolean isFailureBlackListed(String failureMessage, DocWriteRequest<?> request) {
		if (failureMessage != null){
			for (String ex : blackListExceptions){
				if (failureMessage.contains(ex)){
					LOG.error("Black list's exception in script. Exception {}, Requests: {}", failureMessage, request.toString());
					return true;
				}
			}
		}
		return false;
	}

	public DbBulkRequest extractFailedRequestsFromBulk(DbBulkRequest dbBulkRequest, BulkResponse bulkResponses) {
		if (bulkResponses != null){
			// if bulkResponses is null - an exception was thrown while bulking, then all requests failed. No change is needed in the bulk request.
			List<DocWriteRequest<?>> requests = dbBulkRequest.getRequest().requests();
			BulkItemResponse[] responses = bulkResponses.getItems();

			BulkRequest failedRequestsBulk = new BulkRequest();
			int length = requests.size();
			for (int i = 0 ; i < length; i++){
				BulkItemResponse bulkItemResponse = responses[i];
				DocWriteRequest<?> request = requests.get(i);
				if (bulkItemResponse.isFailed() && !isFailureBlackListed(bulkItemResponse.getFailureMessage(), request)){
					failedRequestsBulk.add(request);
				}
			}
			dbBulkRequest = new DbBulkRequest(failedRequestsBulk).setId(dbBulkRequest.getId())
					.setTimesFetched(dbBulkRequest.getTimesFetched()).setInsertTime(dbBulkRequest.getInsertTime());
		}
		return dbBulkRequest;
	}

	public PersistenceHandler getPersistenceHandler() {
		return persistenceHandler;
	}

	public void setPersistenceHandler(PersistenceHandler persistenceHandler) {
		this.persistenceHandler = persistenceHandler;
	}
}

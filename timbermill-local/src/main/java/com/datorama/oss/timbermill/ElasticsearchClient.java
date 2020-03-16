package com.datorama.oss.timbermill;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.PutIndexTemplateRequest;
import org.elasticsearch.client.indices.rollover.RolloverRequest;
import org.elasticsearch.client.indices.rollover.RolloverResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.datorama.oss.timbermill.common.Constants;
import com.datorama.oss.timbermill.common.DbBulkRequest;
import com.datorama.oss.timbermill.common.DiskHandler;
import com.datorama.oss.timbermill.common.ElasticsearchUtil;
import com.datorama.oss.timbermill.unit.Task;
import com.datorama.oss.timbermill.unit.TaskStatus;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.internal.LazilyParsedNumber;

public class ElasticsearchClient {

	private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchClient.class);
    private static final String TTL_FIELD = "meta.dateToDelete";
    private static final String STATUS = "status";
    private static final String WAIT_FOR_COMPLETION = "wait_for_completion";
	private static final boolean withPersistence = true;
	private final RestHighLevelClient client;
    private final int indexBulkSize;
    private static final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
    private final ExecutorService executorService;
    private long maxIndexAge;
    private long maxIndexSizeInGB;
    private long maxIndexDocs;
    private String currentIndex;
    private String oldIndex;
	private int numOfMergedTasksTries;
	private int numOfTasksIndexTries;
	private int maxBulkIndexFetches;
	private LinkedBlockingQueue<Pair<DbBulkRequest, Integer>> failedRequests = new LinkedBlockingQueue<>(100000);
	private DiskHandler diskHandler;

	public ElasticsearchClient(String elasticUrl, int indexBulkSize, int indexingThreads, String awsRegion, String elasticUser, String elasticPassword, long maxIndexAge,
			long maxIndexSizeInGB, long maxIndexDocs, int numOfMergedTasksTries, int numOfBulkIndexTries,int maxBulkIndexFetches, DiskHandler diskHandler) {
		this.diskHandler = diskHandler;
		validateProperties(indexBulkSize, indexingThreads, maxIndexAge, maxIndexSizeInGB, maxIndexDocs, numOfMergedTasksTries, numOfBulkIndexTries);

		this.indexBulkSize = indexBulkSize;
        this.maxIndexAge = maxIndexAge;
        this.maxIndexSizeInGB = maxIndexSizeInGB;
        this.maxIndexDocs = maxIndexDocs;
		this.numOfMergedTasksTries = numOfMergedTasksTries;
		this.maxBulkIndexFetches = maxBulkIndexFetches;
		this.numOfTasksIndexTries = numOfBulkIndexTries;
        this.executorService = Executors.newFixedThreadPool(indexingThreads);
        HttpHost httpHost = HttpHost.create(elasticUrl);
        LOG.info("Connecting to Elasticsearch at url {}", httpHost.toURI());
        RestClientBuilder builder = RestClient.builder(httpHost);
        if (!StringUtils.isEmpty(awsRegion)){
            LOG.info("Trying to connect to AWS Elasticsearch");
            AWS4Signer signer = new AWS4Signer();
            String serviceName = "es";
            signer.setServiceName(serviceName);
            signer.setRegionName(awsRegion);
            HttpRequestInterceptor interceptor = new AWSRequestSigningApacheInterceptor(serviceName, signer, credentialsProvider);
            builder.setHttpClientConfigCallback(callback -> callback.addInterceptorLast(interceptor));
        }

        if (!StringUtils.isEmpty(elasticUser)){
            LOG.info("Connection to Elasticsearch using user {}", elasticUser);
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(elasticUser, elasticPassword));
            builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                    .setDefaultCredentialsProvider(credentialsProvider));
        }

        client = new RestHighLevelClient(builder);
    }

	private void validateProperties(int indexBulkSize, int indexingThreads, long maxIndexAge, long maxIndexSizeInGB, long maxIndexDocs, int numOfMergedTasksTries, int numOfTasksIndexTries) {
		if (indexBulkSize < 1) {
			throw new RuntimeException("Index bulk size property should be larger than 0");
		}
		if (indexingThreads < 1) {
			throw new RuntimeException("Indexing threads property should be larger than 0");
		}
		if (maxIndexAge < 1) {
			throw new RuntimeException("Index max age property should be larger than 0");
		}
		if (maxIndexSizeInGB < 1) {
			throw new RuntimeException("Index max size property should be larger than 0");
		}
		if (maxIndexDocs < 1) {
			throw new RuntimeException("Index max docs property should be larger than 0");
		}
		if (numOfMergedTasksTries < 0) {
			throw new RuntimeException("Max merge tasks retries property should not be below 0");
		}
		if (numOfTasksIndexTries < 0) {
			throw new RuntimeException("Max index tasks retries property should not be below 0");
		}
	}

	public String getCurrentIndex() {
        return currentIndex;
    }

    public String getOldIndex() {
        return oldIndex;
    }

    public void setCurrentIndex(String currentIndex) {
        this.currentIndex = currentIndex;
    }

    public void setOldIndex(String oldIndex) {
        this.oldIndex = oldIndex;
    }

    Map<String, Task> fetchIndexedTasks(String...tasksToFetch) throws IOException {
		if (tasksToFetch.length > 0) {
			return getTasksByIds(tasksToFetch);
		}
		return Collections.emptyMap();
	}

    public Task getTaskById(String taskId){
        try {
			Map<String, Task> tasksByIds = getTasksByIds(taskId);
			return tasksByIds.get(taskId);
		} catch (IOException e){
        	return null;
		}
    }

    public List<Task> getMultipleTasksByIds(String taskId) throws IOException {
        IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery().addIds(taskId);
		Map<String, List<Task>> map = runScrollQuery(null, idsQueryBuilder);
		return map.get(taskId);
    }

    private Map<String, Task> getTasksByIds(String...taskIds) throws IOException {
        IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery();
        for (String taskId : taskIds) {
            idsQueryBuilder.addIds(taskId);
        }
		return getSingleTaskByIds(idsQueryBuilder, null);
    }

    private Map<String, Task> getSingleTaskByIds(AbstractQueryBuilder queryBuilder, String index) throws IOException {
        Map<String, Task> retMap = Maps.newHashMap();
		Map<String, List<Task>> tasks = runScrollQuery(index, queryBuilder);
		for (Map.Entry<String, List<Task>> entry : tasks.entrySet()) {
			List<Task> tasksList = entry.getValue();
			String taskId = entry.getKey();
			if (tasksList.size() == 1){
				retMap.put(taskId, tasksList.get(0));
			}
			else {
				LOG.warn("Fetched multiple tasks per id [{}] from Elasticsearch. Tasks: {}", taskId, tasksList);
			}
		}
		return retMap;
    }

    void indexMetaDataTasks(String env, String... metadataEvents) {
        String index = createTimbermillAlias(env);

        BulkRequest bulkRequest = new BulkRequest();
        for (String metadataEvent : metadataEvents) {
            IndexRequest indexRequest = new IndexRequest(index, Constants.TYPE).source(metadataEvent, XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
        try {
			client.bulk(bulkRequest, RequestOptions.DEFAULT);
		} catch (IllegalStateException e) {
            LOG.warn("Could not index metadata, elasticsearch client was closed.");
		} catch (Throwable t){
			LOG.error("Couldn't index metadata event with events " + Arrays.toString(metadataEvents) + " to elasticsearch cluster.", t);
		}
    }

    public void close(){
        try {
            client.close();
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }
    }

    void sendDbBulkRequest(DbBulkRequest dbBulkRequest, int retryNum){
		BulkRequest request = dbBulkRequest.getRequest();
		int numberOfActions = request.numberOfActions();
		LOG.info("Batch of {} index requests sent to Elasticsearch. Batch size: {} bytes", numberOfActions, request.estimatedSizeInBytes());
		if (retryNum > 0){
			LOG.warn("Retry Number {}/{} for requests of size {}", retryNum, numOfTasksIndexTries, request.estimatedSizeInBytes());
		}
		try {
			BulkResponse responses = bulk(dbBulkRequest,RequestOptions.DEFAULT);
			if (responses.hasFailures()) {
				handleBulkRequestFailure(dbBulkRequest,retryNum,responses,responses.buildFailureMessage());
            }
            else{
                LOG.info("Batch of {} requests finished successfully. Took: {} millis.", numberOfActions, responses.getTook().millis());
            }
        } catch (Throwable t) {
			handleBulkRequestFailure(dbBulkRequest,retryNum,null,t.getMessage());
        }
    }

	BulkResponse bulk(DbBulkRequest request, RequestOptions requestOptions) throws IOException {
		return client.bulk(request.getRequest(), requestOptions);
	}

	public void index(Map<String, Task> tasksMap, String index) {
        Collection<Pair<Future, DbBulkRequest>> futuresRequests = createFuturesRequests(tasksMap, index);

        for (Pair<Future, DbBulkRequest> futureRequest : futuresRequests) {
            try {
				futureRequest.getLeft().get();
            } catch (InterruptedException | ExecutionException e) {
                LOG.error("An error was thrown while indexing a batch", e);
                failedRequests.offer(Pair.of(futureRequest.getRight(), 0));
            }
        }
    }

    void rolloverIndex(String timbermillAlias) {
        RolloverRequest rolloverRequest = new RolloverRequest(timbermillAlias, null);
        rolloverRequest.addMaxIndexAgeCondition(new TimeValue(maxIndexAge, TimeUnit.DAYS));
        rolloverRequest.addMaxIndexSizeCondition(new ByteSizeValue(maxIndexSizeInGB, ByteSizeUnit.GB));
        rolloverRequest.addMaxIndexDocsCondition(maxIndexDocs);
        try {
            RolloverResponse rolloverResponse = client.indices().rollover(rolloverRequest, RequestOptions.DEFAULT);
            if (rolloverResponse.isRolledOver()){
                LOG.info("Index {} rolled over, new index is [{}]", rolloverResponse.getOldIndex(), rolloverResponse.getNewIndex());

                currentIndex = rolloverResponse.getNewIndex();
                oldIndex = rolloverResponse.getOldIndex();

				moveTasksFromOldToNewIndex(1);
            }
        } catch (Throwable t) {
            LOG.error("An error occurred while rollovered index " + timbermillAlias, t);
        }
    }

	private void moveTasksFromOldToNewIndex(int tryNum) {
		try{
			Map<String, Task> previousIndexPartialTasks = getIndexPartialTasks(oldIndex);
			if (!previousIndexPartialTasks.isEmpty()) {
				indexAndDeleteTasks(previousIndexPartialTasks);
			}
		} catch (Throwable t) {
			if (tryNum <= numOfMergedTasksTries) {
				LOG.warn("Try #" + tryNum + " Failed to merge tasks from old index " + oldIndex + ". Going to retry", t);
				try {
					Thread.sleep((2 ^ tryNum) * 1000);
				} catch (InterruptedException ignored) {
				}
				moveTasksFromOldToNewIndex(tryNum + 1);
			}
			else{
				LOG.error("{} tries failed to merge tasks.", tryNum);
			}
		}
    }

	public void indexAndDeleteTasks(Map<String, Task> previousIndexPartialTasks) {
        LOG.info("About to migrate partial tasks from old index [{}] to new index [{}]", oldIndex, currentIndex);
        index(previousIndexPartialTasks, currentIndex);
        deleteTasksFromIndex(previousIndexPartialTasks, oldIndex);
        LOG.info("Successfully migrated {} tasks to new index [{}]", previousIndexPartialTasks.size(), currentIndex);
    }

	private Collection<Pair<Future, DbBulkRequest>> createFuturesRequests(Map<String, Task> tasksMap, String index) {
		Collection<UpdateRequest> requests = createUpdateRequests(tasksMap, index);
		BulkRequest request = new BulkRequest();
		Collection<Pair<Future, DbBulkRequest>> futures = new ArrayList<>();
        for (UpdateRequest updateRequest : requests) {
            request.add(updateRequest);

            if (request.estimatedSizeInBytes() > indexBulkSize) {
				DbBulkRequest dbBulkRequest = new DbBulkRequest(request);
				addRequestToFutures(dbBulkRequest, futures);
                request = new BulkRequest();
            }
        }
        if (!request.requests().isEmpty()) {
			DbBulkRequest dbBulkRequest = new DbBulkRequest(request);
			addRequestToFutures(dbBulkRequest, futures);
        }
//      *** moved to beginning of iteration! ***
//		addRetryTimbermillBulkRequests(futures);
		return futures;
    }

//	private void addRetryTimbermillBulkRequests(Collection<Pair<Future, DbBulkRequest>> futures) {
//		List<DbBulkRequest> failedRequests = diskHandler.fetchFailedBulks();
//		for (DbBulkRequest failedRequest : failedRequests) {
//			addRequestToFutures(failedRequest, futures);
//		}
//	}

	private void addRequestToFutures(DbBulkRequest request, Collection<Pair<Future, DbBulkRequest>> futures) {
        Future<?> future = executorService.submit(() -> sendDbBulkRequest(request, 0));
        futures.add(Pair.of(future, request));
    }

    private Collection<UpdateRequest> createUpdateRequests(Map<String, Task> tasksMap, String index) {
        Collection<UpdateRequest> requests = new ArrayList<>();
        for (Map.Entry<String, Task> taskEntry : tasksMap.entrySet()) {
            Task task = taskEntry.getValue();

            try {
				UpdateRequest updateRequest = task.getUpdateRequest(index, taskEntry.getKey());
				requests.add(updateRequest);
			} catch (Throwable t){
				LOG.error("Failed while creating update request. task:" + task.toString(), t);
			}
        }
        return requests;
    }

    public void bootstrapElasticsearch(int numberOfShards, int numberOfReplicas, int maxTotalFields) {
        putIndexTemplate(numberOfShards, numberOfReplicas, maxTotalFields);
		puStoredScript();
	}

	private void puStoredScript() {
		PutStoredScriptRequest request = new PutStoredScriptRequest();
		request.id(Constants.TIMBERMILL_SCRIPT);
		String content = "{\n"
				+ "  \"script\": {\n"
				+ "    \"lang\": \"painless\",\n"
				+ "    \"source\": \"" + ElasticsearchUtil.SCRIPT
				+ "  }\n"
				+ "}";
		request.content(new BytesArray(content), XContentType.JSON);
		try {
			client.putScript(request, RequestOptions.DEFAULT);
		} catch (Exception e) {
			LOG.error("An error occurred when storing Timbermill index script", e);
			throw new ElasticsearchException(e);
		}
	}

	private void putIndexTemplate(int numberOfShards, int numberOfReplicas, int maxTotalFields) {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest("timbermill-template");

        request.patterns(Lists.newArrayList("timbermill*"));
        request.settings(Settings.builder().put("index.mapping.total_fields.limit", maxTotalFields)
				.put("number_of_shards", numberOfShards)
				.put("number_of_replicas", numberOfReplicas));
		request.mapping(ElasticsearchUtil.MAPPING, XContentType.JSON);
        try {
            client.indices().putTemplate(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            LOG.error("An error occurred when creating Timbermill template", e);
            throw new ElasticsearchException(e);
        }
    }

    String createTimbermillAlias(String env) {
        boolean exists;
        String timbermillAlias = ElasticsearchUtil.getTimbermillIndexAlias(env);
		GetAliasesRequest requestWithAlias = new GetAliasesRequest(timbermillAlias);
		try {
            exists = client.indices().existsAlias(requestWithAlias, RequestOptions.DEFAULT);
        } catch (Throwable t) {
           exists = false;
        }
        if (!exists){
            String initialIndex = getInitialIndex(timbermillAlias);
            CreateIndexRequest request = new CreateIndexRequest(initialIndex);
            Alias alias = new Alias(timbermillAlias);
            request.alias(alias);
            try {
                client.indices().create(request, RequestOptions.DEFAULT);
                currentIndex = initialIndex;
            } catch (Throwable t) {
                LOG.error("error creating Timbermill Alias: " + timbermillAlias + " for index: " + initialIndex, t);
            }
        }
        return timbermillAlias;
    }

    private String getInitialIndex(String timbermillAlias) {
        String initialSerial = ElasticsearchUtil.getIndexSerial(1);
        return timbermillAlias + ElasticsearchUtil.INDEX_DELIMITER + initialSerial;
    }

    public Map<String, Task> fetchTasksByIdsFromIndex(String index, Set<String> ids){
        MultiGetRequest request = new MultiGetRequest();
        for (String id : ids){
            request.add(new MultiGetRequest.Item(index, Constants.TYPE, id));
        }

        Map<String, Task> fetchedTasks = Maps.newHashMap();
        try {
			MultiGetResponse multiGetResponses = client.mget(request, RequestOptions.DEFAULT);
			for (MultiGetItemResponse response : multiGetResponses.getResponses()) {
				if (!response.isFailed()) {
					GetResponse getResponse = response.getResponse();
					if (getResponse.isExists()) {
						String sourceAsString = getResponse.getSourceAsString();
						Task task = Constants.GSON.fromJson(sourceAsString, Task.class);
						fixMetrics(task);
						fetchedTasks.put(response.getId(), task);
					}
				} else {
					LOG.error("Get request for id [{}] failed. Error: {}", response.getId(), response.getFailure().getMessage());
				}
			}
		} catch (Throwable t){
			LOG.error("Couldn't fetch ids: " + ids.toString() + " in index " + index, t);
		}
        return fetchedTasks;
    }

    public Map<String, Task> getIndexPartialTasks(String index) throws IOException {
		TermsQueryBuilder query = new TermsQueryBuilder(STATUS, TaskStatus.PARTIAL_ERROR, TaskStatus.PARTIAL_INFO_ONLY, TaskStatus.PARTIAL_SUCCESS, TaskStatus.UNTERMINATED);
        return getSingleTaskByIds(query, index);
    }

    private Map<String, List<Task>> runScrollQuery(String index, QueryBuilder query) throws IOException {
        SearchRequest searchRequest;
        if (index == null){
            searchRequest = new SearchRequest();
        }
        else {
            searchRequest = new SearchRequest(index);
        }
        Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
        searchRequest.scroll(scroll);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(query);
        searchSourceBuilder.size(10000);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();

        Map<String, List<Task>> tasks = Maps.newHashMap();

        while (searchHits != null && searchHits.length > 0) {
            addHitsToMap(searchHits, tasks);
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);
            searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
        }

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        try {
			ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
			boolean succeeded = clearScrollResponse.isSucceeded();
			if (!succeeded){
				LOG.error("Couldn't clear scroll id [{}] for fetching partial tasks in index [{}]", scrollId, index);
			}
		} catch (Throwable e){
			LOG.error("Couldn't clear scroll id [{}] for fetching partial tasks in index [{}]", scrollId, index);
		}
        return tasks;
    }

    private void addHitsToMap(SearchHit[] searchHits, Map<String, List<Task>> tasks) {
        for (SearchHit searchHit : searchHits) {
            String sourceAsString = searchHit.getSourceAsString();
            Task task = Constants.GSON.fromJson(sourceAsString, Task.class);
            fixMetrics(task);
            String id = searchHit.getId();
            if (!tasks.containsKey(id)){
                tasks.put(id, Lists.newArrayList(task));
            }
            else{
                tasks.get(id).add(task);
            }
        }
    }

    private void fixMetrics(Task task) {
        Map<String, Number> metric = task.getMetric();
        Map<String, Number> newMetrics = new HashMap<>();
        for (Map.Entry<String, Number> entry : metric.entrySet()) {
            Number value = entry.getValue();
            if (value instanceof LazilyParsedNumber){
                if (value.toString().contains(".")){
                    Double newValue = value.doubleValue();
                    newMetrics.put(entry.getKey(), newValue);
                }
                else{
                    Long newValue = value.longValue();
                    newMetrics.put(entry.getKey(), newValue);
                }
            }
        }
        metric.putAll(newMetrics);
    }

    private void deleteTasksFromIndex(Map<String, Task> idToTaskMap, String index) {
        LOG.info("About to delete tasks from  index [{}]", index);
        IdsQueryBuilder idsQuery = new IdsQueryBuilder();
        for (String id : idToTaskMap.keySet()) {
            idsQuery.addIds(id);
        }
        deleteByQuery(index, idsQuery);
    }

    public void deleteExpiredTasks() {
        LOG.info("About to delete expired tasks");
        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder(ElasticsearchClient.TTL_FIELD).lte(new DateTime());
        deleteByQuery("_all", rangeQueryBuilder);
    }

    private void deleteByQuery(String index, QueryBuilder query) {
        DeleteByQueryRequest request = new DeleteByQueryRequest(index).setQuery(query);
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.addHeader(WAIT_FOR_COMPLETION, "false");
        RequestOptions options = builder.build();
        try {
            BulkByScrollResponse bulkByScrollResponse = client.deleteByQuery(request, options);
            LOG.info("Deleted {} tasks.", bulkByScrollResponse.getStatus().getDeleted());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (IllegalStateException e) {
            LOG.warn("Could not perform deletion, elasticsearch client was closed.");
        }
    }

    long countByName(String name, String env) throws IOException {
        CountRequest countRequest = new CountRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("name", name)).must(QueryBuilders.matchQuery("env", env)));
        countRequest.source(searchSourceBuilder);
        CountResponse countResponse = client.count(countRequest, RequestOptions.DEFAULT);
        return countResponse.getCount();
    }

	public void retryFailedRequests() {
		if (!failedRequests.isEmpty()) {
			LOG.info("------------------ Failed Requests From Memory Retry Start ------------------");
			List<Pair<DbBulkRequest, Integer>> list = Lists.newLinkedList();
			failedRequests.drainTo(list);
			for (Pair<DbBulkRequest, Integer> entry : list) {
				DbBulkRequest dbBulkRequest = entry.getKey();
				Integer retryNum = entry.getValue();
				sendDbBulkRequest(dbBulkRequest, retryNum + 1);
			}
			LOG.info("------------------ Failed Requests From Memory Retry End ------------------");
		}
		if (withPersistence && diskHandler.hasFailedBulks()){
			LOG.info("------------------ Failed Requests From Disk Retry Start ------------------");
			List<DbBulkRequest> failedRequestsFromDisk = diskHandler.fetchFailedBulks();
			for (DbBulkRequest dbBulkRequest : failedRequestsFromDisk) {
				sendDbBulkRequest(dbBulkRequest, 0);
			}
			LOG.info("------------------ Failed Requests From Disk Retry End ------------------");
		}

	}

	public boolean hasFailedRequests(){
		boolean hasFailedInMemory = failedRequests.size()>0;
		boolean hasFailedInDisk = diskHandler.hasFailedBulks();
		return hasFailedInMemory || hasFailedInDisk;
	}

	 void handleBulkRequestFailure(DbBulkRequest dbBulkRequest, int retryNum, BulkResponse responses ,String failureMessage){
		if (retryNum >= numOfTasksIndexTries){//numOfTasksIndexTries){
			// failed bulk isn't from disk
			LOG.warn("Reached maximum retries attempt to index for " + dbBulkRequest.getRequest().getDescription(), failureMessage);
			failedRequests.remove(dbBulkRequest);
			if (withPersistence) {
				if (dbBulkRequest.getTimesFetched() < maxBulkIndexFetches){
					remainOnlyFailureRequests(dbBulkRequest, responses);
					diskHandler.persistToDisk(dbBulkRequest);
				}
				else{
					LOG.error("Tasks of bulk {} will not be indexed.",dbBulkRequest.toString());
				}
			}
			else {
				LOG.error("Tasks of bulk {} will not be indexed.",dbBulkRequest.toString());
			}
		}
		else {
			LOG.warn("Failed while trying to bulk index tasks. Going to retry.", failureMessage);
			remainOnlyFailureRequests(dbBulkRequest,responses);
			failedRequests.offer(Pair.of(dbBulkRequest, retryNum));
		}
	}

	private void remainOnlyFailureRequests(DbBulkRequest dbBulkRequest, BulkResponse bulkResponses) {
		if (bulkResponses != null){
			List<DocWriteRequest<?>> requests = dbBulkRequest.getRequest().requests();
			BulkRequest failedRequests = new BulkRequest();
			BulkItemResponse[] responses = bulkResponses.getItems();
			int length = requests.size();
			for (int i = 0 ; i < length; i++){
				if (responses[i].isFailed()){
					failedRequests.add(requests.get(i));
				}
			}
			dbBulkRequest.setRequest(failedRequests);
		}
		//else - the bulk method threw exception, then all requests failed. No change is needed in BulkRequest.
	}

}


package com.datorama.oss.timbermill;

import java.io.IOException;
import java.io.InputStream;
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
import org.elasticsearch.client.*;
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
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.util.IOUtils;
import com.datorama.oss.timbermill.common.Constants;
import com.datorama.oss.timbermill.common.DbBulkRequest;
import com.datorama.oss.timbermill.common.DiskHandler;
import com.datorama.oss.timbermill.common.ElasticsearchUtil;
import com.datorama.oss.timbermill.common.exceptions.MaximunInsertTriesException;
import com.datorama.oss.timbermill.unit.Task;
import com.datorama.oss.timbermill.unit.TaskStatus;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.internal.LazilyParsedNumber;

import static com.datorama.oss.timbermill.common.ElasticsearchUtil.TIMBERMILL_INDEX_PREFIX;

public class ElasticsearchClient {

	private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchClient.class);
    private static final String TTL_FIELD = "meta.dateToDelete";
    private static final String STATUS = "status";
	private boolean withPersistence;
	private final RestHighLevelClient client;
    private final int indexBulkSize;
    private final ExecutorService executorService;
    private long maxIndexAge;
    private long maxIndexSizeInGB;
    private long maxIndexDocs;
    private String currentIndex;
    private String oldIndex;
	private int numOfMergedTasksTries;
	private int numOfBulkIndexTries; // after such number of tries, bulk is considered as failed or will be persisted to disk if has persistence
	private int maxBulkIndexFetches; // after such number of fetches, bulk is considered as failed and won't be persisted anymore
	private LinkedBlockingQueue<Pair<DbBulkRequest, Integer>> failedRequests = new LinkedBlockingQueue<>(100000);
	private DiskHandler diskHandler;

	public ElasticsearchClient(String elasticUrl, int indexBulkSize, int indexingThreads, String awsRegion, String elasticUser, String elasticPassword, long maxIndexAge,
			long maxIndexSizeInGB, long maxIndexDocs, int numOfMergedTasksTries, int numOfBulkIndexTries,int maxBulkIndexFetches,boolean withPersistence, DiskHandler diskHandler) {
		this.diskHandler = diskHandler;
		this.withPersistence = withPersistence;

		validateProperties(indexBulkSize, indexingThreads, maxIndexAge, maxIndexSizeInGB, maxIndexDocs, numOfMergedTasksTries, numOfBulkIndexTries);
		this.indexBulkSize = indexBulkSize;
        this.maxIndexAge = maxIndexAge;
        this.maxIndexSizeInGB = maxIndexSizeInGB;
        this.maxIndexDocs = maxIndexDocs;
		this.numOfMergedTasksTries = numOfMergedTasksTries;
		this.maxBulkIndexFetches = maxBulkIndexFetches;
		this.numOfBulkIndexTries = numOfBulkIndexTries;
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
            HttpRequestInterceptor interceptor = new AWSRequestSigningApacheInterceptor(serviceName, signer, new DefaultAWSCredentialsProviderChain());
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
			Map<String, Task> fetchedTasks = getTasksByIds(currentIndex, tasksToFetch);
			for (String taskId : tasksToFetch) {
				if (!fetchedTasks.containsKey(taskId)){
					LOG.debug("Couldn't find missing parent task with ID {} in Elasticsearch", taskId);
				}
			}
			return fetchedTasks;
		}
		return Collections.emptyMap();
	}

    public Task getTaskById(String taskId){
        try {
			Map<String, Task> tasksByIds = getTasksByIds(null, taskId);
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

    private Map<String, Task> getTasksByIds(String index, String... taskIds) throws IOException {
        IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery();
        for (String taskId : taskIds) {
            idsQueryBuilder.addIds(taskId);
        }
		return getSingleTaskByIds(idsQueryBuilder, index);
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
			LOG.warn("Retry Number {}/{} for requests of size {}", retryNum, numOfBulkIndexTries, request.estimatedSizeInBytes());
		}
		try {
			BulkResponse responses = bulk(dbBulkRequest,RequestOptions.DEFAULT);
			if (responses.hasFailures()) {
				handleBulkRequestFailure(dbBulkRequest,retryNum,responses,responses.buildFailureMessage());
            }
            else{
                LOG.info("Batch with size of {}{} finished successfully. Took: {} millis. Persisted to disk: {}", numberOfActions, dbBulkRequest.getTimesFetched() > 0 ? ", that was fetched from disk," : "", responses.getTook().millis());
            }
        } catch (Throwable t) {
			handleBulkRequestFailure(dbBulkRequest,retryNum,null,t.getMessage());
        }
    }

     // wrap bulk method as a not-final method in order that Mockito will able to mock it
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
					Thread.sleep((long) (Math.pow(2, tryNum) * 1000));
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
		LOG.info("Starting migration between old index [{}] and new index [{}]", oldIndex, currentIndex);
		index(previousIndexPartialTasks, currentIndex);
        deleteTasksFromIndex(previousIndexPartialTasks.keySet(), oldIndex);
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
		return futures;
    }

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
        PutIndexTemplateRequest request = new PutIndexTemplateRequest("timbermill2-template");

        request.patterns(Lists.newArrayList(TIMBERMILL_INDEX_PREFIX + "*"));
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

    private void deleteTasksFromIndex(Set<String> idsSet, String index) {
        LOG.info("Deleting tasks from  index [{}]", index);


        List<String> ids = new ArrayList<>();
		idsSet.forEach(id -> ids.add('"' + id + '"'));
		String query = "{\n"
				+ "    \"query\": {\n"
				+ "        \"ids\" : {\n"
				+ "            \"values\" : " + ids.toString() + " \n"
				+ "        }\n"
				+ "    }\n"
				+ "}";
		deleteByQuery(index, query);
    }

    public void deleteExpiredTasks() {
        LOG.info("About to delete expired tasks");
		String query = "{\n"
				+ "  \"query\": {\n"
				+ "    \"bool\": {\n"
				+ "      \"must\": [\n"
				+ "    {\n"
				+ "    \"range\": {\n"
				+ "      \"" +TTL_FIELD + "\": {\n"
				+ "        \"lte\": \"now\"\n"
				+ "      }\n"
				+ "    }\n"
				+ "    }\n"
				+ "      ]\n"
				+ "    }\n"
				+ "  }\n"
				+ "}";
		deleteByQuery("*", query);
    }

    private JsonElement deleteByQuery(String index, String query) {
		Request request = new Request("POST", "/" + index + "/_delete_by_query");
		request.addParameter("conflicts","proceed");
		request.addParameter("wait_for_completion", "false");
		request.setJsonEntity(query);
		try {
			Response response = client.getLowLevelClient().performRequest(request);
			InputStream content = response.getEntity().getContent();
			String json = IOUtils.toString(content);
			JsonObject asJsonObject = new JsonParser().parse(json).getAsJsonObject();
			JsonElement task = asJsonObject.get("task");
			if (task == null){
				LOG.error("Delete by query didn't return taskId. Response was {}", json);
			}
			else {
				return task;
			}
		} catch (IOException e) {
            throw new RuntimeException(e);
        } catch (IllegalStateException e) {
            LOG.warn("Could not perform deletion, elasticsearch client was closed.");
        }
		return null;
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
			List<Pair<DbBulkRequest, Integer>> list = memoryFailedRequestsAsList();
			for (Pair<DbBulkRequest, Integer> entry : list) {
				DbBulkRequest dbBulkRequest = entry.getKey();
				Integer retryNum = entry.getValue();
				sendDbBulkRequest(dbBulkRequest, retryNum + 1);
			}
			LOG.info("------------------ Failed Requests From Memory Retry End ------------------");
		}
		if (withPersistence && diskHandler.hasFailedBulks()){
			LOG.info("------------------ Failed Requests From Disk Retry Start ------------------");
			List<DbBulkRequest> failedRequestsFromDisk = diskHandler.fetchAndDeleteFailedBulks();
			for (DbBulkRequest dbBulkRequest : failedRequestsFromDisk) {
				sendDbBulkRequest(dbBulkRequest, 0);
			}
			LOG.info("------------------ Failed Requests From Disk Retry End ------------------");
		}

	}

	List<Pair<DbBulkRequest, Integer>> memoryFailedRequestsAsList() {
		List<Pair<DbBulkRequest, Integer>> list = Lists.newLinkedList();
		failedRequests.drainTo(list);
		return list;
	}

	public boolean hasFailedRequests(){
		boolean hasFailedInMemory = failedRequests.size()>0;
		boolean hasFailedInDisk = diskHandler.hasFailedBulks();
		return hasFailedInMemory || hasFailedInDisk;
	}

	 void handleBulkRequestFailure(DbBulkRequest dbBulkRequest, int retryNum, BulkResponse responses ,String failureMessage){
		if (retryNum >= numOfBulkIndexTries){
			LOG.warn("Reached maximum retries ({}) attempt to index. Failure message: {}", numOfBulkIndexTries, failureMessage);
			if (withPersistence) {
				if (dbBulkRequest.getTimesFetched() < maxBulkIndexFetches){
					DbBulkRequest updatedDbBulkRequest = extractFailedRequestsFromBulk(dbBulkRequest, responses);
					try {
						diskHandler.persistToDisk(updatedDbBulkRequest);
					} catch (MaximunInsertTriesException e){
						LOG.error("Tasks of failed bulk will not be indexed because inserting to disk failed for the maximum times ({}).", e.getMaximumTriesNumber());
					}
				}
				else{
					LOG.error("Tasks of failed bulk will not be indexed because it was fetched maximum times ({}).",maxBulkIndexFetches);
				}
			}
			else {
				LOG.error("Tasks of failed bulk will not be indexed because it was fetched maximum times ({}).",maxBulkIndexFetches);
			}
		}
		else {
			LOG.warn("Failed while trying to bulk index tasks, failure message: {}. Going to retry.", failureMessage);
			DbBulkRequest updatedDbBulkRequest = extractFailedRequestsFromBulk(dbBulkRequest,responses);
			failedRequests.offer(Pair.of(updatedDbBulkRequest, retryNum));
		}
	}

	private DbBulkRequest extractFailedRequestsFromBulk(DbBulkRequest dbBulkRequest, BulkResponse bulkResponses) {
		BulkRequest bulkRequest = dbBulkRequest.getRequest();
		int numOfRequests = bulkRequest.numberOfActions();

		if (bulkResponses != null){
			List<DocWriteRequest<?>> requests = dbBulkRequest.getRequest().requests();
			BulkItemResponse[] responses = bulkResponses.getItems();

			BulkRequest failedRequestsBulk = new BulkRequest();
			int length = requests.size();
			for (int i = 0 ; i < length; i++){
				if (responses[i].isFailed()){
					failedRequestsBulk.add(requests.get(i));
				}
			}
			LOG.info("Failed bulk remained with {} failed requests of {}.",requests.size(),numOfRequests);

			dbBulkRequest = new DbBulkRequest(failedRequestsBulk).setId(dbBulkRequest.getId())
					.setTimesFetched(dbBulkRequest.getTimesFetched()).setInsertTime(dbBulkRequest.getInsertTime());
		} else {
			// An exception was thrown while bulking, then all requests failed. No change is needed in the bulk request.
			LOG.info("All {} requests of bulk failed.", numOfRequests, dbBulkRequest.getId());
		}
		return dbBulkRequest;
	}

}


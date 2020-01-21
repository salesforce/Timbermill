package com.datorama.timbermill;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
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

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.AWSRequestSigningApacheInterceptor;
import com.datorama.timbermill.common.TimbermillUtils;
import com.datorama.timbermill.unit.Task;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.internal.LazilyParsedNumber;

import static com.datorama.timbermill.common.Constants.*;

public class ElasticsearchClient {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchClient.class);
    private static final String TTL_FIELD = "meta.dateToDelete";
    private static final String STATUS = "status";
    private static final String WAIT_FOR_COMPLETION = "wait_for_completion";
    private final RestHighLevelClient client;
    private final int indexBulkSize;
    private static final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

    private final ExecutorService executorService;
    private long maxIndexAge;
    private long maxIndexSizeInGB;
    private long maxIndexDocs;
    private String currentIndex;
    private String oldIndex;

    public ElasticsearchClient(String elasticUrl, int indexBulkSize, int indexingThreads, String awsRegion, String elasticUser, String elasticPassword, long maxIndexAge,
            long maxIndexSizeInGB, long maxIndexDocs) {
        this.indexBulkSize = indexBulkSize;
        this.maxIndexAge = maxIndexAge;
        this.maxIndexSizeInGB = maxIndexSizeInGB;
        this.maxIndexDocs = maxIndexDocs;
        if (indexingThreads < 1) {
            indexingThreads = 1;
        }
        this.executorService = Executors.newFixedThreadPool(indexingThreads);
        HttpHost httpHost = HttpHost.create(elasticUrl);
        LOG.info("Connecting to Elasticsearch at url {}", httpHost.toURI());
        LOG.info("$$$$" + System.getenv("elasticsearch.url"));
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

    Map<String, Task> fetchIndexedTasks(Collection<String> tasksToFetch) {
        if (tasksToFetch.isEmpty()) {
            return Collections.emptyMap();
        } else {
            return getTasksByIds(tasksToFetch);
        }
    }

    public Task getTaskById(String taskId) {
        Collection<String> taskIds = new HashSet<>();
        taskIds.add(taskId);
        Map<String, Task> tasksByIds = getTasksByIds(taskIds);
        return tasksByIds.get(taskId);
    }

    public List<Task> getMultipleTasksByIds(String taskId) {
        IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery().addIds(taskId);
        try {
            Map<String, List<Task>> map = runScrollQuery(null, idsQueryBuilder);
            return map.get(taskId);
        } catch (IOException e) {
            LOG.error("Couldn't get Task {} from elasticsearch cluster", taskId);
            throw new ElasticsearchException(e);
        }
    }

    private Map<String, Task> getTasksByIds(Collection<String> taskIds) {
        IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery();
        for (String taskId : taskIds) {
            idsQueryBuilder.addIds(taskId);
        }
        try {
            return getSingleTaskByIds(idsQueryBuilder, null);
        } catch (IOException e) {
            LOG.error("Couldn't get Tasks {} from elasticsearch cluster", taskIds);
            throw new ElasticsearchException(e);
        }
    }

    private Map<String, Task> getSingleTaskByIds(QueryBuilder idsQueryBuilder, String index) throws IOException {
        Map<String, Task> retMap = Maps.newHashMap();
        Map<String, List<Task>> tasks = runScrollQuery(index, idsQueryBuilder);
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
            IndexRequest indexRequest = new IndexRequest(index, TYPE).source(metadataEvent, XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
        try {
            client.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (IOException e){
            LOG.error("Couldn't index metadata event with events " + Arrays.toString(metadataEvents) + " to elasticsearch cluster.", e);
        } catch (IllegalStateException e) {
            LOG.warn("Could not index metadata, elasticsearch client was closed.");
        }
    }

    public void close(){
        try {
            client.close();
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }
    }

    private void sendBulkRequest(BulkRequest request){
        try {
            int numberOfActions = request.numberOfActions();
            LOG.info("Batch of {} index requests sent to Elasticsearch. Batch size: {} bytes", numberOfActions, request.estimatedSizeInBytes());
            BulkResponse responses = client.bulk(request, RequestOptions.DEFAULT);
            if (responses.hasFailures()) {
                LOG.error("Couldn't bulk index tasks to elasticsearch cluster. Error: {}", responses.buildFailureMessage());
            }
            else{
                LOG.info("Batch of {} requests finished successfully. Took: {} millis.", numberOfActions, responses.getTook().millis());
            }
        } catch (IOException | ElasticsearchStatusException e) {
            LOG.error("Couldn't bulk index tasks to elasticsearch cluster.", e);
        }

    }

    public void index(Map<String, Task> tasksMap, String index) {
        Collection<UpdateRequest> updateRequests = createUpdateRequests(tasksMap, index);
        Collection<Future> futuresRequests = createFuturesRequests(updateRequests);

        for (Future futureRequest : futuresRequests) {
            try {
                futureRequest.get();
            } catch (InterruptedException | ExecutionException e) {
                LOG.error("An error was thrown while indexing a batch", e);
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

                new Thread(this::moveTasksFromOldToNewIndex).start();
            }
        } catch (IOException e) {
            LOG.error("An error occurred while rollovered index " + timbermillAlias, e);
        }
    }

    private void moveTasksFromOldToNewIndex(){
        try {
            Map<String, Task> previousIndexPartialTasks = getIndexPartialTasks(oldIndex);
            if (!previousIndexPartialTasks.isEmpty()) {
                indexAndDeleteTasks(previousIndexPartialTasks);
            }
        } catch (IOException e){
            LOG.error("Error while moving tasks from old index [{}] to new index [{}]", oldIndex, currentIndex);
        }
    }

    public void indexAndDeleteTasks(Map<String, Task> previousIndexPartialTasks) {
        LOG.info("About to migrate partial tasks from old index [{}] to new index [{}]", oldIndex, currentIndex);
        index(previousIndexPartialTasks, currentIndex);
        deleteTasksFromIndex(previousIndexPartialTasks, oldIndex);
        LOG.info("Succesfully migrated {} tasks to new index [{}]", previousIndexPartialTasks.size(), currentIndex);
    }

    private Collection<Future> createFuturesRequests(Collection<UpdateRequest> requests) {
        BulkRequest request = new BulkRequest();
        Collection<Future> futures = new ArrayList<>();
        for (UpdateRequest updateRequest : requests) {
            request.add(updateRequest);

            if (request.estimatedSizeInBytes() > indexBulkSize) {
                addRequestToFutures(request, futures);
                request = new BulkRequest();
            }
        }
        if (!request.requests().isEmpty()) {
            addRequestToFutures(request, futures);
        }
        return futures;
    }

    private void addRequestToFutures(BulkRequest request, Collection<Future> futures) {
        Future<?> future = executorService.submit(() -> sendBulkRequest(request));
        futures.add(future);
    }

    private Collection<UpdateRequest> createUpdateRequests(Map<String, Task> tasksMap, String index) {
        Collection<UpdateRequest> requests = new ArrayList<>();
        for (Map.Entry<String, Task> taskEntry : tasksMap.entrySet()) {
            Task task = taskEntry.getValue();

            UpdateRequest updateRequest = task.getUpdateRequest(index, taskEntry.getKey());
            requests.add(updateRequest);
        }
        return requests;
    }

    public void bootstrapElasticsearch(int numberOfShards, int numberOfReplicas) {
        putIndexTemplate(numberOfShards, numberOfReplicas);
		puStoredScript();
	}

	public void puStoredScript() {
		PutStoredScriptRequest request = new PutStoredScriptRequest();
		request.id(TIMBERMILL_SCRIPT);
		String content = "{\n"
				+ "  \"script\": {\n"
				+ "    \"lang\": \"painless\",\n"
				+ "    \"source\": \"" + SCRIPT
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

	private void putIndexTemplate(int numberOfShards, int numberOfReplicas) {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest("timbermill-template");

        request.patterns(Lists.newArrayList("timbermill*"));
        request.settings(Settings.builder()
				.put("number_of_shards", numberOfShards)
				.put("number_of_replicas", numberOfReplicas));
		request.mapping(MAPPING, XContentType.JSON);
        try {
            client.indices().putTemplate(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            LOG.error("An error occurred when creating Timbermill template", e);
            throw new ElasticsearchException(e);
        }
    }

    String createTimbermillAlias(String env) {
        boolean exists;
        String timbermillAlias = TimbermillUtils.getTimbermillIndexAlias(env);
        try {
            GetAliasesRequest requestWithAlias = new GetAliasesRequest(timbermillAlias);
            exists = client.indices().existsAlias(requestWithAlias, RequestOptions.DEFAULT);
        } catch (IOException e) {
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
            } catch (IOException e) {
                throw new RuntimeException("Failed creating initial timbermill index.", e);
            }
        }
        return timbermillAlias;
    }

    private String getInitialIndex(String timbermillAlias) {
        String initialSerial = TimbermillUtils.getIndexSerial(1);
        return timbermillAlias + INDEX_DELIMITER + initialSerial;
    }

    public Map<String, Task> fetchTasksByIdsFromIndex(String index, Set<String> ids) throws IOException {
        MultiGetRequest request = new MultiGetRequest();
        for (String id : ids){
            request.add(new MultiGetRequest.Item(index,TYPE, id));
        }

        Map<String, Task> fetchedTasks = Maps.newHashMap();
        MultiGetResponse multiGetResponses = client.mget(request, RequestOptions.DEFAULT);
        for (MultiGetItemResponse response : multiGetResponses.getResponses()) {
            if (response.isFailed()){
                LOG.error("Get request for id [{}] failed. Error: {}", response.getId(), response.getFailure().getMessage());
            }
            else{
                GetResponse getResponse = response.getResponse();
                if (getResponse.isExists()) {
                    String sourceAsString = getResponse.getSourceAsString();
                    Task task = GSON.fromJson(sourceAsString, Task.class);
                    fixMetrics(task);
                    fetchedTasks.put(response.getId(), task);
                }
            }
        }
        return fetchedTasks;
    }

    public Map<String, Task> getIndexPartialTasks(String index) throws IOException {
        QueryBuilder query = new TermsQueryBuilder(STATUS, Task.TaskStatus.PARTIAL_ERROR, Task.TaskStatus.PARTIAL_INFO_ONLY, Task.TaskStatus.PARTIAL_SUCCESS, Task.TaskStatus.UNTERMINATED); //todo change keyword
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
		} catch (Exception e){
			LOG.error("Couldn't clear scroll id [{}] for fetching partial tasks in index [{}]", scrollId, index);
		}
        return tasks;
    }

    private void addHitsToMap(SearchHit[] searchHits, Map<String, List<Task>> tasks) {
        for (SearchHit searchHit : searchHits) {
            String sourceAsString = searchHit.getSourceAsString();
            Task task = GSON.fromJson(sourceAsString, Task.class);
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

    public long countByName(String name, String env) throws IOException {
        CountRequest countRequest = new CountRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("name", name)).must(QueryBuilders.matchQuery("env", env)));
        countRequest.source(searchSourceBuilder);
        CountResponse countResponse = client.count(countRequest, RequestOptions.DEFAULT);
        return countResponse.getCount();
    }
}

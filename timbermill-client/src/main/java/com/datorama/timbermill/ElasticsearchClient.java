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
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.PutIndexTemplateRequest;
import org.elasticsearch.client.indices.rollover.RolloverRequest;
import org.elasticsearch.client.indices.rollover.RolloverResponse;
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
    private static final String STATUS_KEYWORD = "status.keyword";
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
        RestClientBuilder builder = RestClient.builder(HttpHost.create(elasticUrl));
        if (!StringUtils.isEmpty(awsRegion)){
            LOG.info("Trying to connect to AWS Elasticsearch");
            AWS4Signer signer = new AWS4Signer();
            String serviceName = "es";
            signer.setServiceName(serviceName);
            signer.setRegionName(awsRegion);
            HttpRequestInterceptor interceptor = new AWSRequestSigningApacheInterceptor(serviceName, signer, credentialsProvider);
            builder.setHttpClientConfigCallback(callback -> callback.addInterceptorLast(interceptor));
        }

        if (elasticUser != null){
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
            HashMap<String, Task> retMap = new HashMap<>();
            SearchResponse response = getTasksByIds(tasksToFetch);
            for (SearchHit hit : response.getHits()) {
                Task task = GSON.fromJson(hit.getSourceAsString(), Task.class);
                retMap.put(hit.getId(), task);
            }
            return retMap;
        }
    }

    public Task getTaskById(String taskId) {
        Collection<String> taskIds = new HashSet<>();
        taskIds.add(taskId);
        SearchResponse response = getTasksByIds(taskIds);
        if (response.getHits().getHits().length == 1){
            String sourceAsString = response.getHits().getAt(0).getSourceAsString();
            return GSON.fromJson(sourceAsString, Task.class);
        }
        else {
            return null;
        }
    }

    List<Task> getTasksById(String taskId) {
        Collection<String> taskIds = new HashSet<>();
        taskIds.add(taskId);
        SearchResponse response = getTasksByIds(taskIds);
        List<Task> retList = Lists.newArrayList();
        for (SearchHit hit : response.getHits()) {
            String sourceAsString = hit.getSourceAsString();
            Task task = GSON.fromJson(sourceAsString, Task.class);
            retList.add(task);
        }
        return retList;
    }

    private SearchResponse getTasksByIds(Collection<String> taskIds) {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery();
        for (String taskId : taskIds) {
            idsQueryBuilder.addIds(taskId);
        }
        searchSourceBuilder.query(idsQueryBuilder);
        searchRequest.source(searchSourceBuilder);
        try {
            return client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            LOG.error("Couldn't get Tasks {} from elasticsearch cluster", taskIds);
            throw new ElasticsearchException(e);
        }
    }

    void indexMetaDataTasks(String env, String... metadataEvents) {

        String metadataIndex = TimbermillUtils.getTimbermillIndexAlias(env);

        BulkRequest bulkRequest = new BulkRequest();
        for (String metadataEvent : metadataEvents) {
            IndexRequest indexRequest = new IndexRequest(metadataIndex, TYPE).source(metadataEvent, XContentType.JSON);
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
                LOG.info("Batch of {} requests finished successfully. Took: {} seconds.", numberOfActions, responses.getTook().seconds());
            }
        } catch (IOException e) {
            LOG.error("Couldn't bulk index tasks to elasticsearch cluster.", e);
        }

    }

    public void index(Map<String, Task> tasksMap, String index) {
        BulkRequest bulkRequest = new BulkRequest();
        Collection<UpdateRequest> updateRequests = createUpdateRequests(tasksMap, index);
        Collection<Future> futuresRequests = createFuturesRequests(bulkRequest, updateRequests);

        for (Future futureRequest : futuresRequests) {
            try {
                futureRequest.get();
            } catch (InterruptedException | ExecutionException e) {
                LOG.error("A error was thrown while indexing a batch", e);
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

    private Collection<Future> createFuturesRequests(BulkRequest request, Collection<UpdateRequest> requests) {
        Collection<Future> futures = new ArrayList<>();
        int currentSize = 0;
        for (UpdateRequest updateRequest : requests) {
            request.add(updateRequest);
            currentSize += request.estimatedSizeInBytes();

            if (currentSize > indexBulkSize) {
                addRequestToFutures(request, futures);
                currentSize = 0;
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
        PutIndexTemplateRequest request = new PutIndexTemplateRequest("timbermill-template");
        request.source("{\n"
                + "  \"index_patterns\": [\n"
                + "    \"timbermill*\"\n"
                + "  ],\n"
                + " \"settings\": {\n"
                + "        \"index\": {\n"
                + "            \"number_of_shards\": " + numberOfShards + ",\n"
                + "            \"number_of_replicas\": " + numberOfReplicas + "\n"
                + "        }\n"
                + "    },\n"
                + "    \"mappings\": {\n"
                + "        \"tweets\": {\n"
                + "            \"_source\": {\n"
                + "                \"enabled\": true\n"
                + "            },          \n"
                + "            \"properties\": {\n"
                + "                \"_id\": {\n"
                + "                    \"type\": \"string\"\n"
                + "                },\n"
                + "                \"user\": {\n"
                + "                    \"type\": \"nested\",\n"
                + "                    \"name\": {\n"
                + "                        \"type\": \"string\"\n"
                + "                    }\n"
                + "                }\n"
                + "            }\n"
                + "        }\n"
                + "    }\n"
                + "}"
                + "}", XContentType.JSON);
//        try { //todo fix
//            client.indices().putTemplate(request, RequestOptions.DEFAULT);
//        } catch (IOException e) {
//            LOG.error("An error occurred when creating Timbermill template", e);
//        }
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
        Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.scroll(scroll);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        QueryBuilder query = new TermsQueryBuilder(STATUS_KEYWORD, Task.TaskStatus.PARTIAL_ERROR, Task.TaskStatus.PARTIAL_INFO_ONLY, Task.TaskStatus.PARTIAL_SUCCESS, Task.TaskStatus.UNTERMINATED); //todo change keyword
        searchSourceBuilder.query(query);
        searchSourceBuilder.size(10000);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();

        Map<String, Task> tasks = Maps.newHashMap();

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
        ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        boolean succeeded = clearScrollResponse.isSucceeded();
        if (!succeeded){
            LOG.error("Couldn't clear scroll id [{}] for fetching partial tasks in index [{}]", scrollId, index);
        }
        return tasks;
    }

    private void addHitsToMap(SearchHit[] searchHits, Map<String, Task> tasks) {
        for (SearchHit searchHit : searchHits) {
            String sourceAsString = searchHit.getSourceAsString();
            Task task = GSON.fromJson(sourceAsString, Task.class);
            fixMetrics(task);
            tasks.put(searchHit.getId(), task);
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

}

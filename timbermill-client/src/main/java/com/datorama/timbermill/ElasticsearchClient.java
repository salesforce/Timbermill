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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
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
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.AWSRequestSigningApacheInterceptor;
import com.datorama.timbermill.common.TimbermillUtils;
import com.datorama.timbermill.cron.ExpiredTasksDeletionJob;
import com.datorama.timbermill.unit.Task;

import static com.datorama.timbermill.common.Constants.*;
import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

public class ElasticsearchClient {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchClient.class);
    public static final String CLIENT = "client";
    private final RestHighLevelClient client;
    private final int indexBulkSize;
    private static final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

    private final ExecutorService executorService;
    private long maxIndexAge;
    private long maxIndexSizeInGB;
    private long maxIndexDocs;
    private String deletionCronExp;

    public ElasticsearchClient(String elasticUrl, int indexBulkSize, int indexingThreads, String awsRegion, String elasticUser, String elasticPassword, long maxIndexAge,
            long maxIndexSizeInGB, long maxIndexDocs, String deletionCronExp) {
        this.indexBulkSize = indexBulkSize;
        this.maxIndexAge = maxIndexAge;
        this.maxIndexSizeInGB = maxIndexSizeInGB;
        this.maxIndexDocs = maxIndexDocs;
        this.deletionCronExp = deletionCronExp;
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

    Task getTaskById(String taskId) {
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
            LOG.warn("Could not index metadata, elasicsearch client was closed.");
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

    void index(Map<String, Task> tasksMap, String env) {
        String timbermillAlias = createTimbermillAlias(env);
        BulkRequest bulkRequest = new BulkRequest();
        Collection<UpdateRequest> updateRequests = createUpdateRequests(tasksMap, env);
        Collection<Future> futuresRequests = createFuturesRequests(bulkRequest, updateRequests);

        for (Future futureRequest : futuresRequests) {
            try {
                futureRequest.get();
            } catch (InterruptedException | ExecutionException e) {
                LOG.error("A error was thrown while indexing a batch", e);
            }
        }
        rolloverIndex(timbermillAlias);
    }

    private void rolloverIndex(String timbermillAlias) {
        RolloverRequest rolloverRequest = new RolloverRequest(timbermillAlias, null);
        rolloverRequest.addMaxIndexAgeCondition(new TimeValue(maxIndexAge, TimeUnit.DAYS));
        rolloverRequest.addMaxIndexSizeCondition(new ByteSizeValue(maxIndexSizeInGB, ByteSizeUnit.GB));
        rolloverRequest.addMaxIndexDocsCondition(maxIndexDocs);
        try {
            RolloverResponse rolloverResponse = client.indices().rollover(rolloverRequest, RequestOptions.DEFAULT);
            if (rolloverResponse.isRolledOver()){
                LOG.info("Index {} rolled over, new index is [{}]", rolloverResponse.getOldIndex(), rolloverResponse.getNewIndex());
            }
        } catch (IOException e) {
            LOG.error("An error occurred while rollovered index " + timbermillAlias, e);
        }
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

    private Collection<UpdateRequest> createUpdateRequests(Map<String, Task> tasksMap, String env) {
        Collection<UpdateRequest> requests = new ArrayList<>();
        for (Map.Entry<String, Task> taskEntry : tasksMap.entrySet()) {
            Task task = taskEntry.getValue();

            String index = TimbermillUtils.getTimbermillIndexAlias(env);
            UpdateRequest updateRequest = task.getUpdateRequest(index, taskEntry.getKey());
            requests.add(updateRequest);
        }
        return requests;
    }

    public void bootstrapElasticsearch(int numberOfShards, int numberOfReplicas) { //todo fix
        PutIndexTemplateRequest request = new PutIndexTemplateRequest("timbermill-template");
        request.source("{\n"
                + "  \"index_patterns\": [\n"
                + "    \"timbermill*\"\n"
                + "  ],\n"
                + " \"settings\": {\n"
                + "        \"index\": {\n"
                + "            \"number_of_shards\": 2,\n"
                + "            \"number_of_replicas\": 1\n"
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
        try {
            client.indices().putTemplate(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            LOG.error("An error occurred when creating Timbermill template", e);
        }
    }

    private String createTimbermillAlias(String env) {
        boolean exists;
        String timbermillAlias = TimbermillUtils.getTimbermillIndexAlias(env);
        try {
            GetAliasesRequest requestWithAlias = new GetAliasesRequest(timbermillAlias);
            exists = client.indices().existsAlias(requestWithAlias, RequestOptions.DEFAULT);
        } catch (IOException e) {
           exists = false;
        }
        if (!exists){
            CreateIndexRequest request = new CreateIndexRequest(timbermillAlias + "-000001");
            Alias alias = new Alias(timbermillAlias);
            request.alias(alias);
            try {
                client.indices().create(request, RequestOptions.DEFAULT);
            } catch (IOException e) {
                throw new RuntimeException("Failed creating initail timbermill index.", e);
            }
        }
        return timbermillAlias;
    }

    public void runDeletionTaskCron() {
        try {
            SchedulerFactory sf = new StdSchedulerFactory();
            Scheduler sched = sf.getScheduler();
            JobDataMap jobDataMap = new JobDataMap();
            jobDataMap.put(CLIENT, client);
            JobDetail job = newJob(ExpiredTasksDeletionJob.class)
                    .withIdentity("job1", "group1").usingJobData(jobDataMap)
                    .build();

            CronTrigger trigger = newTrigger()
                    .withIdentity("trigger1", "group1")
                    .withSchedule(cronSchedule(deletionCronExp))
                    .build();

            sched.scheduleJob(job, trigger);
            sched.start();
        } catch (SchedulerException e) {
            LOG.error("Error occurred while expired tasks", e);
        }

    }

}

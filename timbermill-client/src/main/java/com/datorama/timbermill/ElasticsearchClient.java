package com.datorama.timbermill;

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.AWSRequestSigningApacheInterceptor;
import com.datorama.timbermill.unit.Task;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
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
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.datorama.timbermill.common.Constants.*;


class ElasticsearchClient {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchClient.class);
    private final RestHighLevelClient client;
    private final int indexBulkSize;
    private final int daysBackToDelete;
    private static final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

    private final ExecutorService executorService;

    ElasticsearchClient(String elasticUrl, int indexBulkSize, int daysBackToDelete, String awsRegion, int indexingThreads, String elasticUser, String elasticPassword) {
        this.indexBulkSize = indexBulkSize;
        this.daysBackToDelete = daysBackToDelete;
        if (indexingThreads < 1) {
            indexingThreads = 1;
        }
        this.executorService = Executors.newFixedThreadPool(indexingThreads);
        RestClientBuilder builder = RestClient.builder(HttpHost.create(elasticUrl));
        if (!StringUtils.isEmpty(awsRegion)){
            AWS4Signer signer = new AWS4Signer();
            String serviceName = "es";
            signer.setServiceName(serviceName);
            signer.setRegionName(awsRegion);
            HttpRequestInterceptor interceptor = new AWSRequestSigningApacheInterceptor(serviceName, signer, credentialsProvider);
            builder.setHttpClientConfigCallback(callback -> callback.addInterceptorLast(interceptor));
        }

        if (elasticUser != null){
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

    private void deleteOldIndex(String currIndex) throws IOException {
        if (daysBackToDelete < 1){
            return;
        }
        String[] split = currIndex.split("-");
        String indexPrefix = split[0];
        String env = split[1];
        String oldIndex = getTaskIndexWithEnv(indexPrefix, env, ZonedDateTime.now().minusDays(daysBackToDelete));
        GetIndexRequest existsRequest = new GetIndexRequest().indices(oldIndex);
        boolean exists = client.indices().exists(existsRequest, RequestOptions.DEFAULT);
        if (exists) {
            DeleteIndexRequest request = new DeleteIndexRequest(oldIndex);
            client.indices().delete(request, RequestOptions.DEFAULT);
        }
    }

    void indexMetaDataEvents(String env, String... metadataEvents) {
        String metadataIndex = getTaskIndexWithEnv(TIMBERMILL_INDEX_METADATA_PREFIX, env, ZonedDateTime.now());

        BulkRequest bulkRequest = new BulkRequest();
        for (String metadataEvent : metadataEvents) {
            IndexRequest indexRequest = new IndexRequest(metadataIndex, TYPE).source(metadataEvent, XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
        try {
            deleteOldIndex(metadataIndex);
            client.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (IOException e){
            LOG.error("Couldn't index metadata event with events " + Arrays.toString(metadataEvents) + " to elasticsearch cluster.", e);
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
            BulkResponse responses = client.bulk(request, RequestOptions.DEFAULT);
            if (responses.hasFailures()) {
                LOG.error("Couldn't bulk index tasks to elasticsearch cluster. Error: {}", responses.buildFailureMessage());
            }
        } catch (IOException e) {
            LOG.error("Couldn't bulk index tasks to elasticsearch cluster.", e);
        }

    }

    void index(Map<String, Task> tasksMap) {
        BulkRequest request = new BulkRequest();
        int currBatch = 0;

        Collection<UpdateRequest> requests = new ArrayList<>();
        for (Map.Entry<String, Task> taskEntry : tasksMap.entrySet()) {
            Task task = taskEntry.getValue();

            String index = getTaskIndexWithEnv(TIMBERMILL_INDEX_PREFIX, task.getEnv(), ZonedDateTime.now());
            try {
                deleteOldIndex(index);
            } catch (IOException e) {
                LOG.error("Could not delete index " + index, e);
            }
            //TODO not correct - Should be changed to Rollover

            UpdateRequest updateRequest = task.getUpdateRequest(index, taskEntry.getKey());
            requests.add(updateRequest);
        }

        int currentSize = 0;
        Collection<Future> futures = new ArrayList<>();
        for (UpdateRequest updateRequest : requests) {
            request.add(updateRequest);
            currentSize += request.estimatedSizeInBytes();

            if (currentSize > indexBulkSize) {
                BulkRequest finalRequest = request;
                futures.add(executorService.submit(() -> sendBulkRequest(finalRequest)));
                LOG.info("Batch of {} tasks indexed successfully", currBatch);
                currentSize = 0;
                request = new BulkRequest();
            }
        }
        if (!request.requests().isEmpty()) {
            BulkRequest finalRequest = request;
            futures.add(executorService.submit(() -> sendBulkRequest(finalRequest)));
        }

        for (Future future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                LOG.error("A error was thrown while indexing a batch", e);
            }
        }
    }

    private String getTaskIndexWithEnv(String indexPrefix, String env, ZonedDateTime startTime) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy");
        return indexPrefix + '-' + env + '-' + startTime.format(dateTimeFormatter);
    }
}

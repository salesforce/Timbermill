package com.datorama.timbermill;

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.AWSRequestSigningApacheInterceptor;
import com.datorama.timbermill.unit.Event;
import com.datorama.timbermill.unit.Task;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
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
import org.elasticsearch.client.indices.GetIndexRequest;
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

import static com.datorama.timbermill.common.Constants.*;
import static java.util.stream.Collectors.groupingBy;


class ElasticsearchClient {

    private static final int MAX_TRY_NUMBER = 3;
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchClient.class);
    private final RestHighLevelClient client;
    private final int indexBulkSize;
    private final int daysBackToDelete;
    private static final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

    ElasticsearchClient(String elasticUrl, int indexBulkSize, int daysBackToDelete, String awsRegion) {
        this.indexBulkSize = indexBulkSize;
        this.daysBackToDelete = daysBackToDelete;
        RestClientBuilder builder = RestClient.builder(HttpHost.create(elasticUrl));
        if (StringUtils.isEmpty(awsRegion)){
            AWS4Signer signer = new AWS4Signer();
            String serviceName = "es";
            signer.setServiceName(serviceName);
            signer.setRegionName(awsRegion);
            HttpRequestInterceptor interceptor = new AWSRequestSigningApacheInterceptor(serviceName, signer, credentialsProvider);
            builder.setHttpClientConfigCallback(callback -> callback.addInterceptorLast(interceptor));
        }
        client = new RestHighLevelClient(builder);
    }

    Map<String, Task> fetchIndexedTasks(Set<String> eventsToFetch) {
        if (eventsToFetch.isEmpty()) {
            return Collections.emptyMap();
        } else {
            HashMap<String, Task> retMap = new HashMap<>();
            SearchResponse response = getTasksByIds(eventsToFetch);
            for (SearchHit hit : response.getHits()) {
                Task task = GSON.fromJson(hit.getSourceAsString(), Task.class);
                retMap.put(hit.getId(), task);
            }
            return retMap;
        }
    }

    Task getTaskById(String taskId) {
        HashSet<String> taskIds = new HashSet<>();
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

    private SearchResponse getTasksByIds(Set<String> taskIds) {
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
        GetIndexRequest existsRequest = new GetIndexRequest(oldIndex);
        boolean exists = client.indices().exists(existsRequest, RequestOptions.DEFAULT);
        if (exists) {
            DeleteIndexRequest request = new DeleteIndexRequest(oldIndex);
            client.indices().delete(request, RequestOptions.DEFAULT);
        }
    }

    public void indexMetaDataEvents(String env, String...metadataEvents) {
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
            LOG.error("Couldn't index metadata event with events " + metadataEvents.toString() + " to elasticsearch cluster.", e);
        }
    }

    public void close(){
        try {
            client.close();
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }
    }

    private void bulkIndexByBulkSize(List<Event> events, int bulkSize, int tryNum, String env) {
        BulkRequest request = new BulkRequest();
        int currBatch = 0;
        int i = 0;

        List<UpdateRequest> requests = new ArrayList<>();
        Map<String, List<Event>> eventsMap = events.stream().collect(groupingBy(e -> e.getTaskId()));
        for (Map.Entry<String, List<Event>> eventsEntry : eventsMap.entrySet()) {
            Task task = new Task(eventsEntry.getValue(), env);

            String index = getTaskIndexWithEnv(TIMBERMILL_INDEX_PREFIX, env, ZonedDateTime.now());
            try {
                deleteOldIndex(index);
            } catch (IOException e) {
                LOG.error("Could not delete index " + index, e);
            }
            //TODO not correct - Should be changed to Rollover

            UpdateRequest updateRequest = task.getUpdateRequest(index, eventsEntry.getKey());
            requests.add(updateRequest);
        }

        for (UpdateRequest updateRequest : requests) {
            i++;
            currBatch++;

            request.add(updateRequest);
            try{
                if (((i % bulkSize) == 0) || (i == requests.size())) {
                    BulkResponse responses = client.bulk(request, RequestOptions.DEFAULT);

                    if (responses.hasFailures()) {
                        retryUpdateWithSmallerBulkSizeOrFail(events, bulkSize, tryNum, responses.buildFailureMessage(), env);
                    }
                    LOG.info("Batch of {} tasks indexed successfully", currBatch);
                    currBatch = 0;
                    request = new BulkRequest();
                }
            } catch (IOException e) {
                retryUpdateWithSmallerBulkSizeOrFail(events, bulkSize, tryNum, e.getMessage(), env);
            }
        }
    }

    private void retryUpdateWithSmallerBulkSizeOrFail(List<Event> events, int bulkSize, int tryNum, String errorMessage, String env) {
        if (tryNum == MAX_TRY_NUMBER) {
            throw new ElasticsearchException("Couldn't bulk index tasks to elasticsearch cluster after {} tries,"
                    + "Exiting. Error: {}", MAX_TRY_NUMBER, errorMessage);
        } else {
            int smallerBulkSize = (int) Math.ceil((double) bulkSize / 2);
            LOG.warn("Try #{} for indexing failed (with bulk size {}). Will try with smaller batch size of {}. Cause: {}", tryNum, bulkSize, smallerBulkSize, errorMessage);
            bulkIndexByBulkSize(events, smallerBulkSize, tryNum + 1, env);
        }
    }

    void index(List<Event> events, String env) {
        bulkIndexByBulkSize(events, indexBulkSize, 1, env);
    }

    private String getTaskIndexWithEnv(String indexPrefix, String env, ZonedDateTime startTime) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy");
        return indexPrefix + '-' + env + '-' + startTime.format(dateTimeFormatter);
    }
}

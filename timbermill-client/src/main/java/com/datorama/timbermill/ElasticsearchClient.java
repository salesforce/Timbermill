package com.datorama.timbermill;

import com.datorama.timbermill.common.DateTimeTypeConverter;
import com.datorama.timbermill.unit.Task;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static com.datorama.timbermill.common.Constants.*;

public class ElasticsearchClient {

    private static final int MAX_TRY_NUMBER = 3;
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchClient.class);

    private final Gson gson = new GsonBuilder().registerTypeAdapter(DateTime.class, new DateTimeTypeConverter()).create();
    private final RestHighLevelClient client;
    private String env;
    private int indexBulkSize;
    private int daysBackToDelete;

    public ElasticsearchClient(String env, String elasticUrl, int indexBulkSize, int daysBackToDelete) {
        this.indexBulkSize = indexBulkSize;
        this.daysBackToDelete = daysBackToDelete;
        this.env = env;
        client = new RestHighLevelClient(
                RestClient.builder(HttpHost.create(elasticUrl)));
    }

    private String getTaskIndexWithEnv(String indexPrefix, DateTime startTime) {
        return indexPrefix + '-' + env + '-' + startTime.toString("dd-MM-yyyy");
    }

    public Map<String, Task> fetchIndexedTasks(Set<String> eventsToFetch) {
        if (eventsToFetch.isEmpty()) {
            return Collections.emptyMap();
        } else {
            String todayIndex = getTaskIndexWithEnv(TIMBERMILL_INDEX_PREFIX, new DateTime());
            Map<String, Task> retMap = new HashMap<>(fetchIndexedTasksByIndex(eventsToFetch, todayIndex));
            String yesterdayMap = getTaskIndexWithEnv(TIMBERMILL_INDEX_PREFIX, new DateTime().minusDays(1));
            retMap.putAll(fetchIndexedTasksByIndex(eventsToFetch, yesterdayMap));
            return retMap;
        }
    }

    private Map<String, Task> fetchIndexedTasksByIndex(Collection<String> eventsToFetch, String index) {

        GetIndexRequest existsRequest = new GetIndexRequest().indices(index);
        Map<String, Task> ret = new HashMap<>();
        try{
            boolean exists = client.indices().exists(existsRequest, RequestOptions.DEFAULT);
            if (exists){
                MultiGetRequest request = new MultiGetRequest();
                for (String id : eventsToFetch) {
                    request.add(
                            new MultiGetRequest.Item(index, TYPE, id)
                    );
                }
                MultiGetItemResponse[] responses = client.mget(request, RequestOptions.DEFAULT).getResponses();
                for (MultiGetItemResponse response : responses) {
                    if (response.getResponse().isExists()) {
                        String sourceAsString = response.getResponse().getSourceAsString();
                        Task task = gson.fromJson(sourceAsString, Task.class);
                        ret.put(task.getTaskId(), task);
                    }
                }
                LOG.info("Batch of {} tasks fetched successfully", ret.size());
            }
        } catch (Exception e) {
            throw new ElasticsearchException(e);
        }
        return ret;
    }

    public void indexTasks(Map<String, Task> tasksToIndex) {
        if (!tasksToIndex.isEmpty()) {
            Set<String> indices = tasksToIndex.values().stream()
                    .map(t -> getTaskIndexWithEnv(TIMBERMILL_INDEX_PREFIX, t.getStartTime())).collect(Collectors.toSet());
            for (String index : indices){
                createNewIndices(index);
            }
            Task[] tasks = tasksToIndex.values().toArray(new Task[0]);
            bulkIndexByBulkSize(tasks, indexBulkSize, 1);

        }
    }

    private void createNewIndices(String index) {

        GetIndexRequest existsRequest = new GetIndexRequest().indices(index);
        try{
            boolean exists = client.indices().exists(existsRequest, RequestOptions.DEFAULT);
            if (!exists){
                CreateIndexRequest createRequest = new CreateIndexRequest(index);
                client.indices().create(createRequest, RequestOptions.DEFAULT);
                deleteOldIndex(index);
            }
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }
    }

    private void bulkIndexByBulkSize(Task[] tasks, int bulkSize, int tryNum){
        BulkRequest request = new BulkRequest();
        try {
            int currBatch = 0;
            for (int i = 1; i <= tasks.length; i++) {
                currBatch++;
                Task t = tasks[i - 1];
                String index = getTaskIndexWithEnv(TIMBERMILL_INDEX_PREFIX, t.getStartTime());
                IndexRequest req = new IndexRequest(index, TYPE, t.getTaskId()).source(gson.toJson(t), XContentType.JSON);
                request.add(req);

                if (((i % bulkSize) == 0) || (i == tasks.length)) {
                    BulkResponse responses = client.bulk(request, RequestOptions.DEFAULT);

                    if (responses.hasFailures()) {
                        retryIndexWithSmallerBulkSizeOrFail(tasks, bulkSize, tryNum, responses.buildFailureMessage());
                    }
                    LOG.info("Batch of {} tasks indexed successfully", currBatch);
                    currBatch = 0;
                    request = new BulkRequest();
                }
            }
        } catch (IOException e) {
            retryIndexWithSmallerBulkSizeOrFail(tasks, bulkSize, tryNum, e.getMessage());
        }
    }

    private void retryIndexWithSmallerBulkSizeOrFail(Task[] tasks, int bulkSize, int tryNum, String errorMessage) {
        if (tryNum == MAX_TRY_NUMBER) {
            throw new ElasticsearchException("Couldn't bulk index tasks to elsticsearch cluster after {} tries,"
                    + "Exiting. Error: {}", MAX_TRY_NUMBER, errorMessage);
        }
        else{
            int smallerBulkSize = (int) Math.ceil(bulkSize / 2);
            LOG.warn("Try #{} for indexing failed (with bulk size {}). Will try with smaller batch size of {}. Cause: {}", tryNum, bulkSize, smallerBulkSize, errorMessage);
            bulkIndexByBulkSize(tasks, smallerBulkSize, tryNum + 1);
        }
    }

    public void indexTaskToMetaDataIndex(Task task) {
        try {
            String metadataIndex = getTaskIndexWithEnv(TIMBERMILL_INDEX_METADATA_PREFIX, task.getStartTime());
            createNewIndices(metadataIndex);

            IndexRequest indexRequest = new IndexRequest(metadataIndex, TYPE, task.getTaskId()).source(gson.toJson(task), XContentType.JSON);
            client.index(indexRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new ElasticsearchException("Couldn't index task {} to elsticsearch cluster: {}" ,task.getTaskId(), ExceptionUtils.getStackTrace(e));
        }
    }

    public Task getTaskById(String taskId) {

        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.idsQuery().addIds(taskId));
        searchRequest.source(searchSourceBuilder);

        try {
            SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
            if (search.getHits().totalHits == 1){
                String sourceAsString = search.getHits().getAt(0).getSourceAsString();
                return gson.fromJson(sourceAsString, Task.class);
            }
            else {
                return null;
            }
        } catch (IOException e) {
            LOG.error("Couldn't get Task {} from elasticsearch culster", taskId);
            throw new ElasticsearchException(e);
        } catch (NullPointerException e){
            return null;
        }
    }


    private void deleteOldIndex(String currIndex) throws IOException {
        if (daysBackToDelete < 1){
            return;
        }
        String indexPrefix = currIndex.split("-")[0];
        String oldIndex = getTaskIndexWithEnv(indexPrefix, new DateTime().minusDays(daysBackToDelete));
        GetIndexRequest existsRequest = new GetIndexRequest().indices(oldIndex);
        boolean exists = client.indices().exists(existsRequest, RequestOptions.DEFAULT);
        if (exists) {
            DeleteIndexRequest request = new DeleteIndexRequest(oldIndex);
            client.indices().delete(request, RequestOptions.DEFAULT);
        }

    }

    String getEnv() {
        return env;
    }

    public void close() throws IOException {
        client.close();
    }
}

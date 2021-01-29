package com.datorama.oss.timbermill;

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.util.IOUtils;
import com.datorama.oss.timbermill.common.ElasticsearchUtil;
import com.datorama.oss.timbermill.common.KamonConstants;
import com.datorama.oss.timbermill.common.ZonedDateTimeConverter;
import com.datorama.oss.timbermill.common.disk.DbBulkRequest;
import com.datorama.oss.timbermill.common.disk.DiskHandler;
import com.datorama.oss.timbermill.common.disk.IndexRetryManager;
import com.datorama.oss.timbermill.unit.Task;
import com.datorama.oss.timbermill.unit.TaskStatus;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.*;
import com.google.gson.internal.LazilyParsedNumber;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
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
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.io.InputStream;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.datorama.oss.timbermill.common.ElasticsearchUtil.*;
import static org.elasticsearch.action.update.UpdateHelper.ContextFields.CTX;
import static org.elasticsearch.common.Strings.EMPTY_ARRAY;

public class ElasticsearchClient {

	public static final String TYPE = "_doc";
	public static final String TIMBERMILL_SCRIPT = "timbermill-script";
	public static final Gson GSON = new GsonBuilder().registerTypeAdapter(ZonedDateTime.class, new ZonedDateTimeConverter()).create();
	private static final TermsQueryBuilder PARTIALS_QUERY = new TermsQueryBuilder("status", TaskStatus.PARTIAL_ERROR, TaskStatus.PARTIAL_INFO_ONLY, TaskStatus.PARTIAL_SUCCESS);
    private static final String[] ALL_TASK_FIELDS = {"*"};
	private static final String[] PARENT_FIELDS_TO_FETCH = {"name", "parentId", "primaryId", "parentsPath", "orphan", "_index", CTX + ".*"};

	private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchClient.class);
	private static final String TTL_FIELD = "meta.dateToDelete";
    private static final String META_TASK_BEGIN = "meta.taskBegin";
	protected final RestHighLevelClient client;
	private final int indexBulkSize;
	private final ExecutorService executorService;
	private final int numberOfShards;
	private final int maxSlices;
	private long maxIndexAge;
	private long maxIndexSizeInGB;
	private long maxIndexDocs;
	private final int numOfElasticSearchActionsTries;
	private IndexRetryManager retryManager;
	private Bulker bulker;
	private int searchMaxSize;
	private final int scrollLimitation;
	private final int scrollTimeoutSeconds;
	private final int fetchByIdsPartitions;
	private AtomicInteger concurrentScrolls = new AtomicInteger(0);
	private final int expiredMaxIndicesTodeleteInParallel;

	public ElasticsearchClient(String elasticUrl, int indexBulkSize, int indexingThreads, String awsRegion, String elasticUser, String elasticPassword, long maxIndexAge,
			long maxIndexSizeInGB, long maxIndexDocs, int numOfElasticSearchActionsTries, int maxBulkIndexFetches, int searchMaxSize, DiskHandler diskHandler, int numberOfShards, int numberOfReplicas,
			int maxTotalFields, Bulker bulker, int scrollLimitation, int scrollTimeoutSeconds, int fetchByIdsPartitions, int expiredMaxIndicesTodeleteInParallel) {

		if (diskHandler!=null && !diskHandler.isCreatedSuccessfully()){
			diskHandler = null;
		}

		validateProperties(indexBulkSize, indexingThreads, maxIndexAge, maxIndexSizeInGB, maxIndexDocs, numOfElasticSearchActionsTries, numOfElasticSearchActionsTries, scrollLimitation,
				scrollTimeoutSeconds, fetchByIdsPartitions, numberOfShards, expiredMaxIndicesTodeleteInParallel);
		this.indexBulkSize = indexBulkSize;
		this.searchMaxSize = searchMaxSize;
		this.maxIndexAge = maxIndexAge;
        this.maxIndexSizeInGB = maxIndexSizeInGB;
        this.maxIndexDocs = maxIndexDocs;
		this.numOfElasticSearchActionsTries = numOfElasticSearchActionsTries;
        this.executorService = Executors.newFixedThreadPool(indexingThreads);
        this.scrollLimitation = scrollLimitation;
        this.scrollTimeoutSeconds = scrollTimeoutSeconds;
        this.fetchByIdsPartitions = fetchByIdsPartitions;
		this.numberOfShards = numberOfShards;
		this.maxSlices = numberOfShards <= 1 ? 2 : numberOfShards;
		this.expiredMaxIndicesTodeleteInParallel = expiredMaxIndicesTodeleteInParallel;
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
        if (bulker == null){
        	bulker = new Bulker(client);
		}
        this.bulker = bulker;
		this.retryManager = new IndexRetryManager(numOfElasticSearchActionsTries, maxBulkIndexFetches, diskHandler, bulker);
		bootstrapElasticsearch(numberOfShards, numberOfReplicas, maxTotalFields);
    }

    private void validateProperties(int indexBulkSize, int indexingThreads, long maxIndexAge, long maxIndexSizeInGB, long maxIndexDocs, int numOfMergedTasksTries, int numOfElasticSearchActionsTries,
			int scrollLimitation, int scrollTimeoutSeconds, int fetchByIdsPartitions, int numberOfShards, int expiredMaxIndicesToDeleteInParallel) {
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
		if (numOfElasticSearchActionsTries < 0) {
			throw new RuntimeException("Max elasticsearch actions tries property should not be below 0");
		}
		if (scrollLimitation < 0) {
			throw new RuntimeException("Elasticsearch scroll limitation property should not be below 0");
		}
		if (scrollTimeoutSeconds < 1) {
			throw new RuntimeException("Elasticsearch scroll timeout limitation  property should not be below 1");
		}

		if (fetchByIdsPartitions < 1) {
			throw new RuntimeException("Fetch By Ids Partitions property should not be below 1");
		}

		if (numberOfShards < 1) {
			throw new RuntimeException("Number of shards property should not be below 1");
		}

		if (expiredMaxIndicesToDeleteInParallel < 1) {
			throw new RuntimeException("Max Expired Indices To Delete In Parallel property should not be below 1");
		}
	}

	public Task getTaskById(String taskId){
		Map<String, Task> tasksByIds = getTasksByIds(Sets.newHashSet(taskId), "Test", ElasticsearchClient.ALL_TASK_FIELDS,
				org.elasticsearch.common.Strings.EMPTY_ARRAY, TIMBERMILL_INDEX_WILDCARD);
		return tasksByIds.get(taskId);
	}

	public List<Task> getMultipleTasksById(String taskId) {
        IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery().addIds(taskId);
		Map<String, List<Task>> map = Maps.newHashMap();
		List<Future<Map<String, List<Task>>>> futures = runScrollInSlices(idsQueryBuilder, "Test", EMPTY_ARRAY, ALL_TASK_FIELDS, TIMBERMILL_INDEX_WILDCARD);
		for (Future<Map<String, List<Task>>> future : futures) {
			Map<String, List<Task>> taskMap;
			try {
				taskMap = future.get();
			} catch (InterruptedException | ExecutionException e) {
				throw new RuntimeException(e);
			}
			for (Map.Entry<String, List<Task>> entry : taskMap.entrySet()) {
				String key = entry.getKey();
				List<Task> tasks = entry.getValue();
				if (map.putIfAbsent(key, tasks) != null){
					map.get(key).addAll(tasks);
				}
			}
		}
		return map.get(taskId);
    }

	private Map<String, Task> getTasksByIds(Collection<String> taskIds, String functionDescription,
											String[] taskFieldsToInclude, String[] taskFieldsToExclude, String... indices) {
		return getTasksByIds(null, null, taskIds,functionDescription, taskFieldsToInclude, taskFieldsToExclude, indices);
	}

	private Map<String, Task> getTasksByIds(List<QueryBuilder> filterQueryBuilders, List<QueryBuilder> mustNotQueryBuilders, Collection<String> taskIds, String functionDescription,
											String[] taskFieldsToInclude, String[] taskFieldsToExclude, String... indices) {
		Map<String, Task> allTasks = Maps.newHashMap();
		for (List<String> batch : Iterables.partition(taskIds, fetchByIdsPartitions)){
			BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

			IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery();
			for (String taskId : batch) {
				idsQueryBuilder.addIds(taskId);
			}
			boolQueryBuilder.filter(idsQueryBuilder);

			if (filterQueryBuilders != null) {
				for (QueryBuilder filterQueryBuilder : filterQueryBuilders) {
					boolQueryBuilder.filter(filterQueryBuilder);
				}
			}

			if (mustNotQueryBuilders != null) {
				for (QueryBuilder mustNotQueryBuilder : mustNotQueryBuilders) {
					boolQueryBuilder.mustNot(mustNotQueryBuilder);
				}
			}

			Map<String, Task> batchResult = getSingleTaskByIds(boolQueryBuilder, functionDescription, taskFieldsToInclude, taskFieldsToExclude, indices);
			allTasks.putAll(batchResult);
		}
		return allTasks;
    }

    private Map<String, Task> getSingleTaskByIds(AbstractQueryBuilder queryBuilder, String functionDescription, String[] taskFieldsToInclude, String[] taskFieldsToExclude, String... indices) {
        Map<String, Task> retMap = Maps.newHashMap();

		List<Future<Map<String, List<Task>>>> futures = runScrollInSlices(queryBuilder, functionDescription, taskFieldsToInclude, taskFieldsToExclude, indices);

		for (Future<Map<String, List<Task>>> future : futures) {
			try {
				Map<String, List<Task>> tasks = future.get();
				for (Map.Entry<String, List<Task>> entry : tasks.entrySet()) {
					List<Task> tasksList = entry.getValue();
					String taskId = entry.getKey();
					if (tasksList.size() == 1){
						retMap.put(taskId, tasksList.get(0));
					}
					else {
						LOG.warn("Fetched multiple tasks per id [{}] from Elasticsearch for [{}] Tasks: {}", taskId, functionDescription, tasksList);
					}
				}
			} catch (InterruptedException | ExecutionException e) {
				LOG.error("Error while concurrently running sliced scrolls for [" + functionDescription + "]", e);
			}
		}
		return retMap;
    }

	private List<Future<Map<String, List<Task>>>> runScrollInSlices(AbstractQueryBuilder queryBuilder, String functionDescription, String[] taskFieldsToInclude, String[] taskFieldsToExclude,
																	String... indices) {
		List<Future<Map<String, List<Task>>>> futures = Lists.newArrayList();
		for (int sliceId = 0; sliceId < maxSlices; sliceId++) {
			int finalSliceId = sliceId;
			String flowId = MDC.get("id");
			Future<Map<String, List<Task>>> futureFetcher = executorService
					.submit(() -> runScrollQuery(queryBuilder, functionDescription, taskFieldsToInclude, taskFieldsToExclude, flowId, finalSliceId, indices));
			futures.add(futureFetcher);
		}
		return futures;
	}

	void indexMetaDataTasks(String env, Collection<String> metadataEvents) {
        String index = createTimbermillAlias(env);

        BulkRequest bulkRequest = new BulkRequest();
        for (String metadataEvent : metadataEvents) {
            IndexRequest indexRequest = new IndexRequest(index, TYPE).source(metadataEvent, XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
		try {
			runWithRetries(() -> client.bulk(bulkRequest, RequestOptions.DEFAULT) , 1, "Index metadata tasks");
		} catch (MaxRetriesException e) {
			LOG.error("Couldn't index metadata event with events {} to elasticsearch cluster.", metadataEvents.toString());
		}
	}

	public void close(){
        try {
            client.close();
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }
    }

	// return number of failed requests
	public int sendDbFailedBulkRequest(DbBulkRequest request, String flowId, int bulkNum) {
		List<BulkResponse> bulkResponses = sendDbBulkRequest(request, flowId, bulkNum);
		int successfulRequests = 0;
		for (BulkResponse bulkResponse : bulkResponses) {
			if(bulkResponse.hasFailures()){
				for (BulkItemResponse bulkItemResponse : bulkResponse) {
					if (!bulkItemResponse.isFailed()){
						successfulRequests++;
					}
				}
			}
			else{
				successfulRequests += bulkResponse.getItems().length;
			}
		}
		return successfulRequests;
	}

	public List<BulkResponse> sendDbBulkRequest(DbBulkRequest dbBulkRequest, String flowId, int bulkNum) {
		MDC.put("id", flowId);
		return retryManager.indexBulkRequest(dbBulkRequest, bulkNum);
	}
	// wrap bulk method as a not-final method in order that Mockito will able to mock it

	BulkResponse bulk(DbBulkRequest request) throws IOException {
		return bulker.bulk(request);
	}
	//Return number of failed tasks

	public Map<String, String> index(Map<String, Task> tasksMap, String index) {
        Collection<Future<List<BulkResponse>>> futuresRequests = createFuturesIndexRequests(tasksMap, index);

		int bulkNum = 1;
        Map<String, String> overallIdToIndex = Maps.newHashMap();
		for (Future<List<BulkResponse>> futureRequest : futuresRequests) {
            try {
				List<BulkResponse> bulkResponses = futureRequest.get();
				Map<String, String> idToIndexMap = getIdToIndexMap(bulkResponses);
				overallIdToIndex.putAll(idToIndexMap);
			} catch (InterruptedException e) {
				LOG.error("Bulk #{} An error was thrown while indexing a batch, going to retry", bulkNum, e);
			} catch (ExecutionException e) {
				LOG.error("Bulk #{} An error was thrown while indexing a batch, which won't be persisted to disk", bulkNum, e);
			}
			bulkNum++;
        }
		return overallIdToIndex;
    }

	private Map<String, String> getIdToIndexMap(List<BulkResponse> bulkResponses) {
		HashMap<String, String> retMap = Maps.newHashMap();
		for (BulkResponse bulkResponse : bulkResponses) {
			for (BulkItemResponse bulkItemResponse : bulkResponse) {
				if (!bulkItemResponse.isFailed()) {
					retMap.put(bulkItemResponse.getId(), bulkItemResponse.getIndex());
				}
			}
		}
		return retMap;
	}

	void rolloverIndex(String timbermillAlias) {
		try {
			RolloverRequest rolloverRequest = new RolloverRequest(timbermillAlias, null);
			rolloverRequest.addMaxIndexAgeCondition(new TimeValue(maxIndexAge, TimeUnit.DAYS));
			rolloverRequest.addMaxIndexSizeCondition(new ByteSizeValue(maxIndexSizeInGB, ByteSizeUnit.GB));
			rolloverRequest.addMaxIndexDocsCondition(maxIndexDocs);
			RolloverResponse rolloverResponse = (RolloverResponse) runWithRetries(() -> client.indices().rollover(rolloverRequest, RequestOptions.DEFAULT), 1, "Rollover alias " + timbermillAlias);
			if (rolloverResponse.isRolledOver()){
				LOG.info("Alias {} rolled over, new index is [{}]", timbermillAlias, rolloverResponse.getNewIndex());
				updateOldAlias(rolloverResponse, timbermillAlias);
			}
		} catch (Exception e) {
			LOG.error("Could not rollovered alias " + timbermillAlias, e);
		}
    }

	void rolloverIndexForTest(String env){
		try {
			String index = createTimbermillAlias(env);
			RolloverRequest rolloverRequest = new RolloverRequest(index, null);
			rolloverRequest.addMaxIndexDocsCondition(1);
			RolloverResponse rolloverResponse = client.indices().rollover(rolloverRequest, RequestOptions.DEFAULT);
			if (rolloverResponse.isRolledOver()){
				updateOldAlias(rolloverResponse, index);
			}
		} catch (MaxRetriesException | IOException e){
			throw new RuntimeException(e);
		}

	}

	private void updateOldAlias(RolloverResponse rolloverResponse, String timbermillAlias) throws MaxRetriesException {
		String oldAlias = getOldAlias(timbermillAlias);
		if (isAliasExists(oldAlias)) {
			IndicesAliasesRequest removeRequest = new IndicesAliasesRequest();
			IndicesAliasesRequest.AliasActions removeAllIndicesAction = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE).index("*").alias(oldAlias);
			removeRequest.addAliasAction(removeAllIndicesAction);
			AcknowledgedResponse acknowledgedResponse = (AcknowledgedResponse) runWithRetries(() -> client.indices().updateAliases(removeRequest, RequestOptions.DEFAULT),
					1, "Removing old index from alias");
			boolean acknowledged = acknowledgedResponse.isAcknowledged();
			if (!acknowledged) {
				LOG.error("Removing old index from alias [{}] failed", oldAlias);
			}
		}
		IndicesAliasesRequest addRequest = new IndicesAliasesRequest();
		IndicesAliasesRequest.AliasActions addNewOldIndexAction = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD).index(rolloverResponse.getOldIndex()).alias(oldAlias);
		addRequest.addAliasAction(addNewOldIndexAction);
		AcknowledgedResponse acknowledgedResponse = (AcknowledgedResponse) runWithRetries(() -> client.indices().updateAliases(addRequest, RequestOptions.DEFAULT),
				1, "Adding old index to alias");
		boolean acknowledged = acknowledgedResponse.isAcknowledged();
		if (!acknowledged) {
			LOG.error("Adding old index to alias [{}] failed", oldAlias);
		}
	}

	public void migrateTasksToNewIndex() {
		Set<String> indexedEnvs = ElasticsearchUtil.getEnvSet();
		for (String env : indexedEnvs) {
			String currentAlias = ElasticsearchUtil.getTimbermillIndexAlias(env);
			String oldAlias = getOldAlias(currentAlias);

			try {
				if (isAliasExists(currentAlias)) {
					if (isAliasExists(oldAlias)) {
						//Find matching tasks from old index to partial tasks in new index
						Set<String> currentIndexPartialsIds = findPartialsIds(currentAlias);
						Map<String, Task> matchedTasksFromOld = getTasksByIds(currentIndexPartialsIds,
								"Fetch matched tasks from old index " + oldAlias, ALL_TASK_FIELDS, org.elasticsearch.common.Strings.EMPTY_ARRAY, oldAlias);
						logPartialsMetadata(currentAlias, currentIndexPartialsIds, matchedTasksFromOld);

						//Find partials tasks from old that have matching tasks in new, excluding already found tasks
						Set<String> oldIndexPartialsIds = findPartialsIds(oldAlias);
						oldIndexPartialsIds.removeAll(currentIndexPartialsIds);
						Map<String, Task> matchingTasksNew = getTasksByIds(oldIndexPartialsIds,
								"Fetch matched ids from current index " + currentAlias, EMPTY_ARRAY, ALL_TASK_FIELDS, currentAlias);
						Set<String> oldMatchedIndexPartialsIds = matchingTasksNew.keySet();
						Map<String, Task> matchedTasksToMigrateFromOld = getTasksByIds(oldMatchedIndexPartialsIds,
								"Fetch partials tasks from old index " + oldAlias, ALL_TASK_FIELDS, org.elasticsearch.common.Strings.EMPTY_ARRAY, oldAlias);
						logPartialsMetadata(oldAlias, oldIndexPartialsIds, matchedTasksToMigrateFromOld);

						Map<String, Task> tasksToMigrateIntoNewIndex = Maps.newHashMap();
						tasksToMigrateIntoNewIndex.putAll(matchedTasksFromOld);
						tasksToMigrateIntoNewIndex.putAll(matchedTasksToMigrateFromOld);
						indexAndDeleteTasks(tasksToMigrateIntoNewIndex, oldAlias, currentAlias);

					} else {
						LOG.info("Old alias {} doesn't exists.", oldAlias);
					}
				} else {
					LOG.error("Main alias {} doesn't exists.", currentAlias);
				}
			} catch (MaxRetriesException | ExecutionException | InterruptedException e){
				LOG.error("Failed running migration cron for main alias [" + currentAlias + "] and old alias [" + oldAlias + "]", e);
			}
		}
	}

	private void logPartialsMetadata(String index, Set<String> IndexPartialsIds, Map<String, Task> matchedTasks) {
		LOG.info("Found {} partials tasks in index {} with {} that can be migrated.", IndexPartialsIds.size(), index, matchedTasks.size());
		KamonConstants.PARTIAL_TASKS_FOUND_HISTOGRAM.withTag("index", index).record(IndexPartialsIds.size());
		KamonConstants.PARTIAL_TASKS_MIGRATED_HISTOGRAM.withTag("index", index).record(matchedTasks.size());
	}

	Map<String, Task> getMissingParents(Set<String> parentIds, String env) {
		String timbermillAlias = ElasticsearchUtil.getTimbermillIndexAlias(env);
		String oldAlias = getOldAlias(timbermillAlias);
		return getTasksByIds(parentIds, "Fetch missing parents tasks", PARENT_FIELDS_TO_FETCH, null, timbermillAlias, oldAlias);
	}

	private Set<String> findPartialsIds(String index) {
		BoolQueryBuilder latestPartialsQuery = getLatestPartialsQuery();
		Map<String, Task> singleTaskByIds = getSingleTaskByIds(latestPartialsQuery, "Get partials from index " + index, EMPTY_ARRAY, ALL_TASK_FIELDS, index);
		return singleTaskByIds.keySet();
	}

	private boolean isAliasExists(String currentIndex) throws MaxRetriesException {
		GetAliasesRequest requestWithAlias = new GetAliasesRequest(currentIndex);
		GetAliasesResponse response = (GetAliasesResponse) runWithRetries(() -> client.indices().getAlias(requestWithAlias, RequestOptions.DEFAULT), 1,
				"Is Timbermill alias exists");
		return !response.getAliases().isEmpty();
	}

	private BoolQueryBuilder getLatestPartialsQuery() {
		BoolQueryBuilder latestPartialsQuery = QueryBuilders.boolQuery();
		latestPartialsQuery.filter(QueryBuilders.rangeQuery(META_TASK_BEGIN).to("now-1m"));
		latestPartialsQuery.filter(PARTIALS_QUERY);
		return latestPartialsQuery;
	}

	private void indexAndDeleteTasks(Map<String, Task> tasksToMigrateIntoNewIndex, String oldIndex, String currentIndex) throws ExecutionException, InterruptedException {
		if (!tasksToMigrateIntoNewIndex.isEmpty()) {
			LOG.info("Migrating {} tasks to new index [{}]", tasksToMigrateIntoNewIndex.size(), currentIndex);

			Collection<Future<List<BulkResponse>>> futuresRequests = createFuturesIndexRequests(tasksToMigrateIntoNewIndex, currentIndex);

			int failedRequests = 0;
			for (Future<List<BulkResponse>> futureRequest : futuresRequests) {
				List<BulkResponse> bulkResponses = futureRequest.get();
				for (BulkResponse bulkResponse : bulkResponses) {
					if (bulkResponse.hasFailures()){
						for (BulkItemResponse bulkItemResponse : bulkResponse) {
							if(bulkItemResponse.isFailed()){
								failedRequests++;
							}
						}
					}
				}
			}


			if (failedRequests > 0){
				LOG.info("There were {} failed migration requests", failedRequests);
				KamonConstants.PARTIAL_TASKS_FAILED_TO_MIGRATED_HISTOGRAM.withoutTags().record(failedRequests);
			}
			deleteTasksFromIndex(tasksToMigrateIntoNewIndex.keySet(), oldIndex);
		}
    }

	private Collection<Future<List<BulkResponse>>> createFuturesIndexRequests(Map<String, Task> tasksMap, String index) {
		Collection<UpdateRequest> requests = createUpdateRequests(tasksMap, index);
		BulkRequest request = new BulkRequest();
        Collection<Future<List<BulkResponse>>> futures = new ArrayList<>();
		int bulkNum = 1;
        for (UpdateRequest updateRequest : requests) {
            request.add(updateRequest);

			if (request.estimatedSizeInBytes() > indexBulkSize) {
				Future<List<BulkResponse>> future = createFutureTask(request, bulkNum);
				futures.add(future);
				request = new BulkRequest();
                bulkNum++;
            }
        }
        if (!request.requests().isEmpty()) {
			Future<List<BulkResponse>> future = createFutureTask(request, bulkNum);
            futures.add(future);
        }
		return futures;
    }

    private Future<List<BulkResponse>> createFutureTask(BulkRequest request, int bulkNum) {
        DbBulkRequest dbBulkRequest = new DbBulkRequest(request);
		String flowId = MDC.get("id");
		return executorService.submit(() -> sendDbBulkRequest(dbBulkRequest, flowId, bulkNum));
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

    private void bootstrapElasticsearch(int numberOfShards, int numberOfReplicas, int maxTotalFields) {
		try {
			putIndexTemplate(numberOfShards, numberOfReplicas, maxTotalFields);
			puStoredScript();
		} catch (MaxRetriesException e) {
			throw new RuntimeException(e);
		}
	}

	private void puStoredScript() throws MaxRetriesException {
		PutStoredScriptRequest request = new PutStoredScriptRequest();
		request.id(TIMBERMILL_SCRIPT);
		String content = "{\n"
				+ "  \"script\": {\n"
				+ "    \"lang\": \"painless\",\n"
				+ "    \"source\": \"" + ElasticsearchUtil.SCRIPT
				+ "  }\n"
				+ "}";
		request.content(new BytesArray(content), XContentType.JSON);
		runWithRetries(() -> client.putScript(request, RequestOptions.DEFAULT), 1, "Put Timbermill stored script");
	}

	private void putIndexTemplate(int numberOfShards, int numberOfReplicas, int maxTotalFields) throws MaxRetriesException {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest("timbermill2-template");

        request.patterns(Lists.newArrayList(TIMBERMILL_INDEX_WILDCARD));
        request.settings(Settings.builder().put("index.mapping.total_fields.limit", maxTotalFields)
				.put("number_of_shards", numberOfShards)
				.put("number_of_replicas", numberOfReplicas));
		request.mapping(ElasticsearchUtil.MAPPING, XContentType.JSON);
		runWithRetries(() -> client.indices().putTemplate(request, RequestOptions.DEFAULT), 1, "Put Timbermill Index Template");
    }

    public String createTimbermillAlias(String env) {
        String timbermillAlias = ElasticsearchUtil.getTimbermillIndexAlias(env);
		String initialIndex = getInitialIndex(timbermillAlias);
		try {
			if (!isAliasExists(timbermillAlias)) {
				CreateIndexRequest request = new CreateIndexRequest(initialIndex);
				Alias alias = new Alias(timbermillAlias);
				request.alias(alias);
				runWithRetries(() -> client.indices().create(request, RequestOptions.DEFAULT), 1, "Create index alias " + timbermillAlias + " for index " + initialIndex);
			}
		} catch (MaxRetriesException e){
			LOG.error("Failed creating Timbermill Alias {}, going to use index {}", timbermillAlias, initialIndex);
			return initialIndex;
		}
		return timbermillAlias;
	}

    private String getInitialIndex(String timbermillAlias) {
        String initialSerial = ElasticsearchUtil.getIndexSerial(1);
        return timbermillAlias + INDEX_DELIMITER + initialSerial;
    }

	private Map<String, List<Task>> runScrollQuery(QueryBuilder query, String functionDescription, String[] taskFieldsToInclude, String[] taskFieldsToExclude, String flowId, int sliceId, String...indices){
		MDC.put("id", flowId);
		SearchRequest searchRequest = createSearchRequest(query, taskFieldsToInclude, taskFieldsToExclude, sliceId, indices);
		List<SearchResponse> searchResponses = new ArrayList<>();
		Set<String> scrollIds = Sets.newHashSet();
		try {
			SearchResponse searchResponse = (SearchResponse) runWithRetries(() -> client.search(searchRequest, RequestOptions.DEFAULT), 1, "Initial search for " + functionDescription);
			if (searchResponse.getFailedShards() > 0){
				LOG.warn("Scroll search failed some shards for {}. First error was {}", functionDescription, searchResponse.getShardFailures()[0].toString());
			}
			String scrollId = searchResponse.getScrollId();
			LOG.debug("Scroll ID {} opened. Open scrolls {}", scrollId.length() > 100 ? scrollId.substring(0, 100) : scrollId, concurrentScrolls.incrementAndGet());
			scrollIds.add(scrollId);
			searchResponses.add(searchResponse);
			SearchHit[] searchHits = searchResponse.getHits().getHits();
			boolean keepScrolling = searchHits != null && searchHits.length > 0;
			Stopwatch stopWatch = Stopwatch.createUnstarted();
			int numOfScrollsPerformed = 0;
			boolean timeoutReached = false;
			boolean numOfScrollsReached = false;
			while (shouldKeepScrolling(searchResponse, timeoutReached, numOfScrollsReached)) {
				SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
				scrollRequest.scroll(TimeValue.timeValueSeconds(30L));
				stopWatch.start();
				searchResponse = (SearchResponse) runWithRetries(() -> client.scroll(scrollRequest, RequestOptions.DEFAULT), 1, "Scroll search for scroll id: " + scrollId + " for " + functionDescription);
				stopWatch.stop();
				if (!searchResponse.getScrollId().equals(scrollId)){
					concurrentScrolls.incrementAndGet();
					scrollId = searchResponse.getScrollId();
					scrollIds.add(scrollId);
				}
				LOG.debug("Scroll ID {} Scroll search. Open scrolls {}", scrollId.length() > 100 ? scrollId.substring(0, 100) : scrollId, concurrentScrolls.get());
				searchResponses.add(searchResponse);
				timeoutReached = stopWatch.elapsed(TimeUnit.SECONDS) > scrollTimeoutSeconds;
				numOfScrollsReached = ++numOfScrollsPerformed >= scrollLimitation;
				if (timeoutReached && keepScrolling) {
					LOG.error("Scroll timeout limit of [{} seconds] reached", scrollTimeoutSeconds);
				}
				if (numOfScrollsReached && keepScrolling) {
					LOG.error("Scrolls amount  limit of [{} scroll operations] reached", scrollLimitation);
				}
			}

		}
		catch (MaxRetriesException e) {
			// return what managed to be found before failing.
		}
		finally {
			clearScroll(functionDescription, scrollIds);
		}
		return addHitsToMap(searchResponses);
    }

	private void clearScroll(String functionDescription, Set<String> scrollIds) {
		if (!scrollIds.isEmpty()) {
			ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
			for (String scrollId : scrollIds) {
				clearScrollRequest.addScrollId(scrollId);
			}

			try {
				ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
				boolean succeeded = clearScrollResponse.isSucceeded();
				if (!succeeded) {
					LOG.error("Couldn't clear one of scroll ids {} for [{}]", scrollIds, functionDescription);
				}
				else{
					final StringBuilder s = new StringBuilder();
					scrollIds.forEach(scrollId -> s.append(scrollId.length() > 100 ? scrollId.substring(0, 100) : scrollId + "      |     "));
					LOG.debug("Scroll ID set: {} closed. Open scrolls {}", s.toString(), concurrentScrolls.addAndGet( 0 - scrollIds.size()));
				}
			} catch (Throwable e) {
				LOG.error("Couldn't clear one of scroll ids " + scrollIds + " for [" + functionDescription + "]", e);
			}
		}
	}

	private boolean shouldKeepScrolling(SearchResponse searchResponse, boolean timeoutReached, boolean numOfScrollsReached) {
		SearchHit[] searchHits = searchResponse.getHits().getHits();
		return searchHits != null && searchHits.length > 0 && !timeoutReached && !numOfScrollsReached;
	}

	private SearchRequest createSearchRequest(QueryBuilder query, String[] taskFieldsToInclude, String[] taskFieldsToExclude, int sliceId, String...indices) {
		SearchRequest searchRequest = new SearchRequest(indices);
		searchRequest.scroll(TimeValue.timeValueSeconds(30L));
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

		SliceBuilder sliceBuilder = new SliceBuilder(META_TASK_BEGIN, sliceId, maxSlices);
		searchSourceBuilder.slice(sliceBuilder);
		searchSourceBuilder.fetchSource(taskFieldsToInclude, taskFieldsToExclude);
		searchSourceBuilder.query(query);
		searchSourceBuilder.size(searchMaxSize);
		searchRequest.source(searchSourceBuilder);
		searchRequest.source().timeout(new TimeValue(30, TimeUnit.SECONDS));
		return searchRequest;
	}

	private Object runWithRetries(Callable<Object> callable, int tryNum, String functionDescription) throws MaxRetriesException {
		if (tryNum > 1) {
			LOG.info("Started try # {}/{} for [{}]", tryNum, numOfElasticSearchActionsTries, functionDescription);
		}
		try {
			return callable.call();
		} catch (Exception e) {
			if (tryNum < numOfElasticSearchActionsTries){
				double sleep = Math.pow(2, tryNum);
				LOG.warn("Failed try # " + tryNum + "/" + numOfElasticSearchActionsTries + " for [" + functionDescription + "] Going to sleep for " + sleep + " seconds.", e);
				try {
					Thread.sleep((long) (sleep * 1000)); //Exponential backoff
				} catch (InterruptedException ignored) {
				}
				return runWithRetries(callable, tryNum + 1, functionDescription);
			}
			else{
				LOG.error("Reached maximum retries (" + numOfElasticSearchActionsTries + ") attempts for [" + functionDescription + "]", e);
				throw new MaxRetriesException(e);
			}
		}
	}

	private Map<String, List<Task>> addHitsToMap(List<SearchResponse> searchResponses) {
		Map<String, List<Task>> tasks = Maps.newHashMap();
		for (SearchResponse searchResponse : searchResponses) {
			SearchHit[] hits = searchResponse.getHits().getHits();
			for (SearchHit searchHit : hits) {
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
		return tasks;
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
        List<String> ids = new ArrayList<>();
		idsSet.forEach(id -> ids.add('"' + id + '"'));
		String query = "{\n"
				+ "        \"ids\" : {\n"
				+ "            \"values\" : " + ids.toString() + " \n"
				+ "        }\n"
				+ "    }";
		deleteByQuery(index, query);
    }

    public void deleteExpiredTasks() {
        LOG.info("About to delete expired tasks");
		String query = "{\n"
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
				+ "  }";
		List<String> indicesToDelete = findIndicesWithExpiredTasks();
		if (indicesToDelete != null && !indicesToDelete.isEmpty()) {
			for (String indexToDelete : indicesToDelete) {
				deleteByQuery(indexToDelete, query);
			}
		}
    }

	private List<String> findIndicesWithExpiredTasks() {
		BucketOrder descCountOrder = InternalOrder.count(false);
		TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("indices").field("_index").order(descCountOrder).size(expiredMaxIndicesTodeleteInParallel);
		QueryBuilder query = new RangeQueryBuilder(TTL_FIELD).lte("now");
		SearchSourceBuilder searchBuilder = new SearchSourceBuilder().aggregation(termsAggregationBuilder).query(query);
		SearchRequest searchRequest = new SearchRequest(TIMBERMILL_INDEX_WILDCARD).source(searchBuilder);
		SearchResponse searchResponse;
		try {
			searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		Aggregations aggregations = searchResponse.getAggregations();
		if (aggregations != null) {
			ParsedStringTerms termsAgg = aggregations.get("indices");
			List<? extends Terms.Bucket> buckets = termsAgg.getBuckets();
			if (buckets != null) {
				return buckets.stream().map(MultiBucketsAggregation.Bucket::getKeyAsString).collect(Collectors.toList());
			}
		}
		return null;
	}

	private void deleteByQuery(String index, String query) {
		Request request = new Request("POST", "/" + index + "/_delete_by_query");
		request.addParameter("conflicts","proceed");
		request.addParameter("wait_for_completion", "false");
		request.addParameter("slices", Integer.toString(numberOfShards));
		String fullQuery = "{\n"
				+ "    \"query\": " + query + "\n"
				+ "}";
		request.setJsonEntity(fullQuery);
		try {
			Response response = client.getLowLevelClient().performRequest(request);
			InputStream content = response.getEntity().getContent();
			String json = IOUtils.toString(content);
			JsonObject asJsonObject = new JsonParser().parse(json).getAsJsonObject();
			JsonElement task = asJsonObject.get("task");
			if (task != null) {
				LOG.info("Task id {} for deletion by query", task);
			} else {
				LOG.error("Delete by query didn't return taskId. Response was {}", json);
			}
		} catch (Exception e) {
            LOG.warn("Could not perform deletion.", e);
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

    public Bulker getBulker() {
		return bulker;
	}

	public IndexRetryManager getRetryManager() {
		return retryManager;
	}
}


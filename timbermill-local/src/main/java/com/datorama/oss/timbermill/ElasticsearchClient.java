package com.datorama.oss.timbermill;

import java.io.IOException;
import java.io.InputStream;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
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
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import static com.datorama.oss.timbermill.common.ElasticsearchUtil.TIMBERMILL_INDEX_PREFIX;
import static org.elasticsearch.action.update.UpdateHelper.ContextFields.CTX;
import static org.elasticsearch.common.Strings.EMPTY_ARRAY;

public class ElasticsearchClient {

	public static final String TYPE = "_doc";
	public static final String TIMBERMILL_SCRIPT = "timbermill-script";
	public static final Gson GSON = new GsonBuilder().registerTypeAdapter(ZonedDateTime.class, new ZonedDateTimeConverter()).create();
	private static final TermsQueryBuilder PARTIALS_QUERY = new TermsQueryBuilder("status", TaskStatus.PARTIAL_ERROR, TaskStatus.PARTIAL_INFO_ONLY, TaskStatus.PARTIAL_SUCCESS, TaskStatus.UNTERMINATED);
	private static final TermQueryBuilder ORPHANS_QUERY = QueryBuilders.termQuery("orphan", true);
	private static final String[] ALL_TASK_FIELDS = {"*"};

	private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchClient.class);
	private static final String TTL_FIELD = "meta.dateToDelete";
	private static final String[] PARENT_FIELDS_TO_FETCH = { "env", "parentId", "orphan", "primaryId", CTX + ".*", "parentsPath", "name"};
	private final RestHighLevelClient client;
	private final int indexBulkSize;
	private final ExecutorService executorService;
	private long maxIndexAge;
	private long maxIndexSizeInGB;
	private long maxIndexDocs;
	private String currentIndex;
	private String oldIndex;
	private final int numOfElasticSearchActionsTries;
	private IndexRetryManager retryManager;
	private Bulker bulker;
	private int searchMaxSize;
	private final int scrollLimitation;
	private final int scrollTimeoutSeconds;
	private final int fetchByIdsPartitions;
	private AtomicInteger concurrentScrolls = new AtomicInteger(0);

	private final ReadWriteLock indexRolloverValuesAccessController;

	public ElasticsearchClient(String elasticUrl, int indexBulkSize, int indexingThreads, String awsRegion, String elasticUser, String elasticPassword, long maxIndexAge,
			long maxIndexSizeInGB, long maxIndexDocs, int numOfElasticSearchActionsTries, int maxBulkIndexFetches, int searchMaxSize, DiskHandler diskHandler, int numberOfShards, int numberOfReplicas,
			int maxTotalFields, Bulker bulker, int scrollLimitation, int scrollTimeoutSeconds, int fetchByIdsPartitions) {

		if (diskHandler!=null && !diskHandler.isCreatedSuccessfully()){
			diskHandler = null;
		}

		validateProperties(indexBulkSize, indexingThreads, maxIndexAge, maxIndexSizeInGB, maxIndexDocs, numOfElasticSearchActionsTries, numOfElasticSearchActionsTries, scrollLimitation, scrollTimeoutSeconds, fetchByIdsPartitions);
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
		this.indexRolloverValuesAccessController = new ReentrantReadWriteLock();
    }

    public Map<String, Task> getMissingParents(Set<String> startEventsIds, Set<String> parentIds, String flowId) {
		LOG.debug("Flow ID: [{}] Fetching {} missing parents", flowId, parentIds.size());

		parentIds.removeAll(startEventsIds);
        Map<String, Task> previouslyIndexedParentTasks = Maps.newHashMap();
        try {
			if (!parentIds.isEmpty()) {
				previouslyIndexedParentTasks = getNonOrphansTasksByIds(parentIds, flowId);
			}
        } catch (Throwable t) {
            LOG.error("Flow ID: [{}] Error fetching indexed tasks from Elasticsearch", flowId, t);
        }
        LOG.debug("Flow ID: [{}] Fetched {} parents", flowId, previouslyIndexedParentTasks.size());
        return previouslyIndexedParentTasks;
    }

	private void validateProperties(int indexBulkSize, int indexingThreads, long maxIndexAge, long maxIndexSizeInGB, long maxIndexDocs, int numOfMergedTasksTries, int numOfElasticSearchActionsTries,
			int scrollLimitation, int scrollTimeoutSeconds, int fetchByIdsPartitions) {
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
	}

	public boolean doesIndexAlreadyRolledOver() {
		indexRolloverValuesAccessController.readLock().lock();
		boolean result = null != oldIndex;
		indexRolloverValuesAccessController.readLock().unlock();
		return  result;
	}

	public String unsafeGetCurrentIndex() {
        return currentIndex;
    }

    public String unsafeGetOldIndex() {
        return oldIndex;
    }

    public void setRollOveredIndicesValues(String oldIndex, String currentIndex) {
		//blocking
		indexRolloverValuesAccessController.writeLock().lock();
		this.currentIndex = currentIndex;
		this.oldIndex = oldIndex;
		indexRolloverValuesAccessController.writeLock().unlock();
	}

	public Task getTaskById(String taskId){
		Map<String, Task> tasksByIds = getTasksByIds(TIMBERMILL_INDEX_PREFIX + "*", Sets.newHashSet(taskId), "Test", ALL_TASK_FIELDS, EMPTY_ARRAY, "test");
		return tasksByIds.get(taskId);
	}

	public List<Task> getMultipleTasksByIds(String taskId) {
        IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery().addIds(taskId);
		Map<String, List<Task>> map = runScrollQuery(TIMBERMILL_INDEX_PREFIX + "*", idsQueryBuilder, "Test", EMPTY_ARRAY, ALL_TASK_FIELDS, "test");
		return map.get(taskId);
    }

	private Map<String, Task> getTasksByIds(String index, Collection<String> taskIds, String functionDescription, String[] taskFieldsToInclude,
			String[] taskFieldsToExclude, String flowId) {
		return getTasksByIds(index, new BoolQueryBuilder(), taskIds, functionDescription, taskFieldsToInclude, taskFieldsToExclude, flowId);
	}

    private Map<String, Task> getTasksByIds(String index, BoolQueryBuilder queryBuilder, Collection<String> taskIds, String functionDescription, String[] taskFieldsToInclude,
			String[] taskFieldsToExclude, String flowId) {
		Map<String, Task> allTasks = Maps.newHashMap();
		for (List<String> batch : Iterables.partition(taskIds, fetchByIdsPartitions)){
			IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery();
			for (String taskId : batch) {
				idsQueryBuilder.addIds(taskId);
			}
			queryBuilder.filter(idsQueryBuilder);
			Map<String, Task> batchResult = getSingleTaskByIds(queryBuilder, index, functionDescription, taskFieldsToInclude, taskFieldsToExclude, flowId);
			allTasks.putAll(batchResult);
		}
		return allTasks;
    }

	private Map<String, Task> getNonOrphansTasksByIds(Collection<String> taskIds, String flowId) {
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		ExistsQueryBuilder startedTaskQueryBuilder = QueryBuilders.existsQuery("primaryId");

		boolQueryBuilder.filter(startedTaskQueryBuilder);
		boolQueryBuilder.mustNot(ORPHANS_QUERY);
		return getTasksByIds(TIMBERMILL_INDEX_PREFIX + "*", boolQueryBuilder, taskIds, "Fetch previously indexed parent tasks", ElasticsearchClient.PARENT_FIELDS_TO_FETCH, EMPTY_ARRAY, flowId);
	}

	public Map<String, Task> getLatestOrphanIndexed(int partialTasksGraceMinutes, int orphansFetchPeriodMinutes, String flowId) {
		Set<String> envsToFilterOn = getIndexedEnvs();
		BoolQueryBuilder finalOrphansQuery = QueryBuilders.boolQuery();
		RangeQueryBuilder partialOrphansRangeQuery = buildRelativeRangeQuery(partialTasksGraceMinutes);
		RangeQueryBuilder allOrphansRangeQuery = buildRelativeRangeQuery(orphansFetchPeriodMinutes, partialTasksGraceMinutes);
		TermsQueryBuilder envsQuery = QueryBuilders.termsQuery("env", envsToFilterOn);

		BoolQueryBuilder nonPartialOrphansQuery = QueryBuilders.boolQuery();
		nonPartialOrphansQuery.filter(ORPHANS_QUERY);
		nonPartialOrphansQuery.filter(envsQuery);
		nonPartialOrphansQuery.filter(partialOrphansRangeQuery);
		nonPartialOrphansQuery.mustNot(PARTIALS_QUERY);

		BoolQueryBuilder orphansWithoutPartialLimitationQuery = QueryBuilders.boolQuery();
		orphansWithoutPartialLimitationQuery.filter(ORPHANS_QUERY);
		orphansWithoutPartialLimitationQuery.filter(envsQuery);
		orphansWithoutPartialLimitationQuery.filter(allOrphansRangeQuery);

		finalOrphansQuery.should(orphansWithoutPartialLimitationQuery);
		finalOrphansQuery.should(nonPartialOrphansQuery);

		return getSingleTaskByIds(finalOrphansQuery, TIMBERMILL_INDEX_PREFIX + "*", "Fetch latest indexed orphans", PARENT_FIELDS_TO_FETCH, EMPTY_ARRAY, flowId);

	}

	private Map<String, Task> getSingleTaskByIds(AbstractQueryBuilder queryBuilder, String index, String functionDescription, String[] taskFieldsToInclude, String[] taskFieldsToExclude,
			String flowId) {
        Map<String, Task> retMap = Maps.newHashMap();
		Map<String, List<Task>> tasks = runScrollQuery(index, queryBuilder, functionDescription, taskFieldsToInclude, taskFieldsToExclude, flowId);
		for (Map.Entry<String, List<Task>> entry : tasks.entrySet()) {
			List<Task> tasksList = entry.getValue();
			String taskId = entry.getKey();
			if (tasksList.size() == 1){
				retMap.put(taskId, tasksList.get(0));
			}
			else {
				LOG.warn("Flow ID: [{}] Fetched multiple tasks per id [{}] from Elasticsearch for [{}] Tasks: {}", flowId, taskId, functionDescription, tasksList);
			}
		}
		return retMap;
    }

    void indexMetaDataTasks(String env, Collection<String> metadataEvents, String flowId) {
        String index = createTimbermillAlias(env, flowId);

        BulkRequest bulkRequest = new BulkRequest();
        for (String metadataEvent : metadataEvents) {
            IndexRequest indexRequest = new IndexRequest(index, TYPE).source(metadataEvent, XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
		try {
			runWithRetries(() -> client.bulk(bulkRequest, RequestOptions.DEFAULT) , 1, "Index metadata tasks", flowId);
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
	public int sendDbBulkRequest(DbBulkRequest dbBulkRequest, String flowId, int bulkNum) {
		BulkRequest request = dbBulkRequest.getRequest();
		int numberOfActions = request.numberOfActions();
		LOG.debug("Flow ID: [{}] Bulk #{} Batch of {} index requests sent to Elasticsearch. Batch size: {} bytes", flowId, bulkNum, numberOfActions, request.estimatedSizeInBytes());

		try {
			BulkResponse responses = bulk(dbBulkRequest);
			if (responses.hasFailures()) {
				return retryManager.retrySendDbBulkRequest(dbBulkRequest,responses,responses.buildFailureMessage(), flowId, bulkNum);
			}
			LOG.debug("Flow ID: [{}] Bulk #{} Batch of {} index requests finished successfully. Took: {} millis.", flowId, bulkNum, numberOfActions, responses.getTook().millis());
			if (dbBulkRequest.getTimesFetched() > 0 ){
				KamonConstants.TASKS_FETCHED_FROM_DISK_HISTOGRAM.withTag("outcome","success").record(1);
			}
			return 0;
		} catch (Throwable t) {
			return retryManager.retrySendDbBulkRequest(dbBulkRequest,null,t.getMessage(), flowId, bulkNum);
		}
	}

	// wrap bulk method as a not-final method in order that Mockito will able to mock it
	BulkResponse bulk(DbBulkRequest request) throws IOException {
		return bulker.bulk(request);
	}

	//Return number of failed tasks
	public int index(Map<String, Task> tasksMap, String index, String flowId) {
        Collection<Pair<Future<Integer>, DbBulkRequest>> futuresRequests = createFuturesRequests(tasksMap, index, flowId);

		int bulkNum = 1;
		int overallFailedRequests = 0;
		for (Pair<Future<Integer>, DbBulkRequest> futureRequest : futuresRequests) {
			try {
				Integer failedRequests = futureRequest.getLeft().get();
				overallFailedRequests += failedRequests;
			} catch (InterruptedException e) {
				LOG.error("Flow ID: [{}] Bulk #{} An error was thrown while indexing a batch, going to retry", flowId, bulkNum, e);
				sendDbBulkRequest(futureRequest.getRight(), flowId, bulkNum);
			} catch (ExecutionException e) {
				LOG.error("Flow ID: [{}] Bulk #{} An error was thrown while indexing a batch, which won't be persisted to disk", flowId, bulkNum, e);
			}
			bulkNum++;
        }
		return overallFailedRequests;
    }

    void rolloverIndex(String timbermillAlias, String flowId) {
		try {
			RolloverRequest rolloverRequest = new RolloverRequest(timbermillAlias, null);
			rolloverRequest.addMaxIndexAgeCondition(new TimeValue(maxIndexAge, TimeUnit.DAYS));
			rolloverRequest.addMaxIndexSizeCondition(new ByteSizeValue(maxIndexSizeInGB, ByteSizeUnit.GB));
			rolloverRequest.addMaxIndexDocsCondition(maxIndexDocs);
			RolloverResponse rolloverResponse = (RolloverResponse) runWithRetries(() -> client.indices().rollover(rolloverRequest, RequestOptions.DEFAULT), 1, "Rollover alias " + timbermillAlias,
					flowId);
			if (rolloverResponse.isRolledOver()){
				LOG.info("Flow ID: [{}] Alias {} rolled over, new index is [{}]", flowId, timbermillAlias, rolloverResponse.getNewIndex());
				setRollOveredIndicesValues(rolloverResponse.getOldIndex(), rolloverResponse.getNewIndex());

			}
		} catch (Exception e) {
			LOG.error("Flow ID: [{}] Could not rollovered alias {}", flowId, timbermillAlias);
		}
    }

	public void migrateTasksToNewIndex(int relativeMinutes, String flowId) {
		Map<String, Task> tasksToMigrateIntoNewIndex = Maps.newHashMap();

		indexRolloverValuesAccessController.readLock().lock();
		String oldIndex = this.oldIndex;
		String currentIndex = this.currentIndex;
		indexRolloverValuesAccessController.readLock().unlock();

		Map<String, Task> matchingPartialsTasksFromOldIndex = findMatchingTasksToMigrate(relativeMinutes, oldIndex, currentIndex, flowId);
		Map<String, Task> matchingPartialsTasksFromNewIndex = findMatchingTasksToMigrate(relativeMinutes, currentIndex, oldIndex, flowId);
		tasksToMigrateIntoNewIndex.putAll(matchingPartialsTasksFromOldIndex);
		tasksToMigrateIntoNewIndex.putAll(matchingPartialsTasksFromNewIndex);
		indexAndDeleteTasks(tasksToMigrateIntoNewIndex, oldIndex, currentIndex, flowId);
	}

	private Map<String,Task> findMatchingTasksToMigrate(int relativeMinutes, String indexOfPartials, String indexOfMatchingTasks, String flowId) {
		BoolQueryBuilder latestPartialsQuery = getLatestPartialsQuery(relativeMinutes);

		Map<String, Task> matchingTasks = Maps.newHashMap();
		String functionDescription = "Migrate old tasks to new index";
		Map<String, Task> singleTaskByIds = getSingleTaskByIds(latestPartialsQuery, indexOfPartials, functionDescription, EMPTY_ARRAY, ALL_TASK_FIELDS, flowId);
		if (!singleTaskByIds.isEmpty()) {
			matchingTasks = getTasksByIds(indexOfMatchingTasks, singleTaskByIds.keySet(), functionDescription, ALL_TASK_FIELDS, EMPTY_ARRAY, flowId);

			KamonConstants.PARTIAL_TASKS_FOUND_HISTOGRAM.withoutTags().record(singleTaskByIds.size());
			LOG.info("Flow ID: [{}] Found {} partials tasks in index {}.", flowId, matchingTasks.size(), indexOfPartials);
		}
		return matchingTasks;
	}

	private BoolQueryBuilder getLatestPartialsQuery(int relativeMinutes) {
		BoolQueryBuilder latestPartialsQuery = QueryBuilders.boolQuery();
		RangeQueryBuilder latestItemsQuery = buildRelativeRangeQuery(relativeMinutes);
		TermsQueryBuilder envsQuery = QueryBuilders.termsQuery("env", getIndexedEnvs());

		latestPartialsQuery.filter(latestItemsQuery);
		latestPartialsQuery.filter(envsQuery);
		latestPartialsQuery.filter(PARTIALS_QUERY);
		return latestPartialsQuery;
	}

	private void indexAndDeleteTasks(Map<String, Task> tasksToMigrateIntoNewIndex, String oldIndex, String currentIndex, String flowId) {
		if (!tasksToMigrateIntoNewIndex.isEmpty()) {
			LOG.info("Flow ID: [{}] Migrating {} tasks to new index [{}]", flowId, tasksToMigrateIntoNewIndex.size(), currentIndex);
			int failedRequests = index(tasksToMigrateIntoNewIndex, currentIndex, flowId);
			if (failedRequests > 0){
				LOG.info("Flow ID: [{}] There were {} failed migration requests", flowId, failedRequests);
				KamonConstants.PARTIAL_TASKS_FAILED_TO_MIGRATED_HISTOGRAM.withoutTags().record(failedRequests);
			}
			deleteTasksFromIndex(tasksToMigrateIntoNewIndex.keySet(), oldIndex, flowId);
			KamonConstants.PARTIAL_TASKS_MIGRATED_HISTOGRAM.withoutTags().record(tasksToMigrateIntoNewIndex.size());
		}
    }

	private Collection<Pair<Future<Integer>, DbBulkRequest>> createFuturesRequests(Map<String, Task> tasksMap, String index, String flowId) {
		Collection<UpdateRequest> requests = createUpdateRequests(tasksMap, index, flowId);
		BulkRequest request = new BulkRequest();
		Collection<Pair<Future<Integer>, DbBulkRequest>> futures = new ArrayList<>();
		int bulkNum = 1;
        for (UpdateRequest updateRequest : requests) {
            request.add(updateRequest);

            if (request.estimatedSizeInBytes() > indexBulkSize) {
				DbBulkRequest dbBulkRequest = new DbBulkRequest(request);
				addRequestToFutures(dbBulkRequest, futures, flowId, bulkNum);
                request = new BulkRequest();
                bulkNum++;
            }
        }
        if (!request.requests().isEmpty()) {
			DbBulkRequest dbBulkRequest = new DbBulkRequest(request);
			addRequestToFutures(dbBulkRequest, futures, flowId, bulkNum);
        }
		return futures;
    }

	private void addRequestToFutures(DbBulkRequest request, Collection<Pair<Future<Integer>, DbBulkRequest>> futures, String flowId, int bulkNum) {
        Future<Integer> future = executorService.submit(() -> {return sendDbBulkRequest(request, flowId, bulkNum);});
        futures.add(Pair.of(future, request));
    }

    private Collection<UpdateRequest> createUpdateRequests(Map<String, Task> tasksMap, String index, String flowId) {
        Collection<UpdateRequest> requests = new ArrayList<>();
        for (Map.Entry<String, Task> taskEntry : tasksMap.entrySet()) {
            Task task = taskEntry.getValue();

            try {
				UpdateRequest updateRequest = task.getUpdateRequest(index, taskEntry.getKey());
				requests.add(updateRequest);
			} catch (Throwable t){
				LOG.error("Flow ID: [" + flowId + "] Failed while creating update request. task:" + task.toString(), t);
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
		runWithRetries(() -> client.putScript(request, RequestOptions.DEFAULT), 1, "Put Timbermill stored script", "bootstrap");
	}

	private void putIndexTemplate(int numberOfShards, int numberOfReplicas, int maxTotalFields) throws MaxRetriesException {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest("timbermill2-template");

        request.patterns(Lists.newArrayList(TIMBERMILL_INDEX_PREFIX + "*"));
        request.settings(Settings.builder().put("index.mapping.total_fields.limit", maxTotalFields)
				.put("number_of_shards", numberOfShards)
				.put("number_of_replicas", numberOfReplicas));
		request.mapping(ElasticsearchUtil.MAPPING, XContentType.JSON);
		runWithRetries(() -> client.indices().putTemplate(request, RequestOptions.DEFAULT), 1, "Put Timbermill Index Template", "bootstrap");
    }

    public String createTimbermillAlias(String env, String flowId) {
        String timbermillAlias = ElasticsearchUtil.getTimbermillIndexAlias(env);
		String initialIndex = getInitialIndex(timbermillAlias);
		GetAliasesRequest requestWithAlias = new GetAliasesRequest(timbermillAlias);
		try {
			GetAliasesResponse response = (GetAliasesResponse) runWithRetries(() -> client.indices().getAlias(requestWithAlias, RequestOptions.DEFAULT), 1,
					"Get Timbermill alias for env " + env, flowId);
			boolean exists = !response.getAliases().isEmpty();
			if (!exists) {
				CreateIndexRequest request = new CreateIndexRequest(initialIndex);
				Alias alias = new Alias(timbermillAlias);
				request.alias(alias);
				runWithRetries(() -> client.indices().create(request, RequestOptions.DEFAULT), 1, "Create index alias " + timbermillAlias + " for index " + initialIndex, flowId);
				setRollOveredIndicesValues(null, initialIndex);
			}
		} catch (MaxRetriesException e){
			LOG.error("Flow ID: [{}] Failed creating Timbermill Alias {}, going to use index {}",flowId, timbermillAlias, initialIndex);
			return initialIndex;
		}
		return timbermillAlias;
	}

    private String getInitialIndex(String timbermillAlias) {
        String initialSerial = ElasticsearchUtil.getIndexSerial(1);
        return timbermillAlias + ElasticsearchUtil.INDEX_DELIMITER + initialSerial;
    }

	private Map<String, List<Task>> runScrollQuery(String index, QueryBuilder query, String functionDescription, String[] taskFieldsToInclude, String[] taskFieldsToExclude, String flowId){
		SearchRequest searchRequest = createSearchRequest(index, query, taskFieldsToInclude, taskFieldsToExclude);
		searchRequest.source().timeout(new TimeValue(30, TimeUnit.SECONDS));
		List<SearchResponse> searchResponses = new ArrayList<>();
		Set<String> scrollIds = Sets.newHashSet();
		try {
			SearchResponse searchResponse = (SearchResponse) runWithRetries(() -> client.search(searchRequest, RequestOptions.DEFAULT), 1, "Initial search for " + functionDescription, flowId);
			if (searchResponse.getFailedShards() > 0){
				LOG.error("Flow ID: [{}] Scroll search failed some shards for {}. First error was {}", flowId, functionDescription, searchResponse.getShardFailures()[0].toString());
			}
			String scrollId = searchResponse.getScrollId();
			LOG.info("Flow ID: [{}] Scroll ID {} opened. Open scrolls {}", flowId, scrollId.substring(0, 100), concurrentScrolls.incrementAndGet());
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
				LOG.info("Flow ID: [{}] Scroll ID {} Scroll search", flowId, scrollId.substring(0, 100));
				scrollRequest.scroll(TimeValue.timeValueSeconds(30L));
				stopWatch.start();
				searchResponse = (SearchResponse) runWithRetries(() -> client.scroll(scrollRequest, RequestOptions.DEFAULT), 1, "Scroll search for scroll id: " + scrollId + " for " + functionDescription,
						flowId);
				stopWatch.stop();
				scrollId = searchResponse.getScrollId();
				scrollIds.add(scrollId);
				searchResponses.add(searchResponse);
				timeoutReached = stopWatch.elapsed(TimeUnit.SECONDS) > scrollTimeoutSeconds;
				numOfScrollsReached = ++numOfScrollsPerformed >= scrollLimitation;
				if (timeoutReached && keepScrolling) {
					LOG.error("Flow ID: [{}] Scroll timeout limit of [{} seconds] reached", flowId, scrollTimeoutSeconds);
				}
				if (numOfScrollsReached && keepScrolling) {
					LOG.error("Flow ID: [{}] Scrolls amount  limit of [{} scroll operations] reached", flowId, scrollLimitation);
				}
			}

		}
		catch (MaxRetriesException e) {
			// return what managed to be found before failing.
		}
		finally {
			clearScroll(index, functionDescription, scrollIds, flowId);
		}
		return addHitsToMap(searchResponses);
    }

	private void clearScroll(String index, String functionDescription, Set<String> scrollIds, String flowId) {
		if (!scrollIds.isEmpty()) {
			if (scrollIds.size() > 1) {
				LOG.info("Scroll ID set is bigger than 1");
			}
			ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
			for (String scrollId : scrollIds) {
				clearScrollRequest.addScrollId(scrollId);
			}
			try {
				ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
				boolean succeeded = clearScrollResponse.isSucceeded();
				if (!succeeded) {
					LOG.error("Flow ID: [{}] Couldn't clear one of scroll ids {} for [{}] in index {}", flowId, scrollIds, functionDescription, index);
				}
				else{
					LOG.info("Scroll ID set: {} closed. Open scrolls {}", scrollIds, concurrentScrolls.addAndGet( 0 - scrollIds.size()));
				}
			} catch (Throwable e) {
				LOG.error("Flow ID: [" + flowId + "] Couldn't clear one of scroll ids " + scrollIds + " for [" + functionDescription + "] in index " + index, e);
			}
		}
	}

	private boolean shouldKeepScrolling(SearchResponse searchResponse, boolean timeoutReached, boolean numOfScrollsReached) {
		SearchHit[] searchHits = searchResponse.getHits().getHits();
		return searchHits != null && searchHits.length > 0 && !timeoutReached && !numOfScrollsReached;
	}

	private SearchRequest createSearchRequest(String index, QueryBuilder query, String[] taskFieldsToInclude, String[] taskFieldsToExclude) {
		SearchRequest searchRequest;
		if (index == null){
			searchRequest = new SearchRequest();
		}
		else {
			searchRequest = new SearchRequest(index);
		}
		searchRequest.scroll(TimeValue.timeValueSeconds(30L));
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.fetchSource(taskFieldsToInclude, taskFieldsToExclude);
		searchSourceBuilder.query(query);
		searchSourceBuilder.size(searchMaxSize);
		searchRequest.source(searchSourceBuilder);
		return searchRequest;
	}

	private ActionResponse runWithRetries(Callable<ActionResponse> callable, int tryNum, String functionDescription, String flowId) throws MaxRetriesException {
		if (tryNum > 1) {
			LOG.info("Flow ID: [{}] Started try # {}/{} for [{}]", flowId, tryNum, numOfElasticSearchActionsTries, functionDescription);
		}
		try {
			return callable.call();
		} catch (Exception e) {
			if (tryNum < numOfElasticSearchActionsTries){
				double sleep = Math.pow(2, tryNum);
				LOG.warn("Flow ID: [" + flowId + "] Failed try # " + tryNum + "/" + numOfElasticSearchActionsTries + " for [" + functionDescription + "] Going to sleep for " + sleep + " seconds.", e);
				try {
					Thread.sleep((long) (sleep * 1000)); //Exponential backoff
				} catch (InterruptedException ignored) {
				}
				return runWithRetries(callable, tryNum + 1, functionDescription, flowId);
			}
			else{
				LOG.error("Flow ID: [" + flowId + "] Reached maximum retries (" + numOfElasticSearchActionsTries + ") attempts for [" + functionDescription + "]", e);
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
				task.setIndex(searchHit.getIndex());
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

    private void deleteTasksFromIndex(Set<String> idsSet, String index, String flowId) {
        List<String> ids = new ArrayList<>();
		idsSet.forEach(id -> ids.add('"' + id + '"'));
		String query = "{\n"
				+ "        \"ids\" : {\n"
				+ "            \"values\" : " + ids.toString() + " \n"
				+ "        }\n"
				+ "    }";
		deleteByQuery(index, query, flowId);
    }

    public void deleteExpiredTasks(String flowId) {
        LOG.info("Flow ID: [{}] About to delete expired tasks", flowId);
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
		deleteByQuery("*", query, flowId);
    }

    private void deleteByQuery(String index, String query, String flowId) {
		Request request = new Request("POST", "/" + index + "/_delete_by_query");
		request.addParameter("conflicts","proceed");
		request.addParameter("wait_for_completion", "false");
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
				LOG.info("Flow ID: [{}] Task id {} for deletion by query", flowId, task);
			} else {
				LOG.error("Flow ID: [{}] Delete by query didn't return taskId. Response was {}", flowId, json);
			}
		} catch (Exception e) {
            LOG.warn("Flow ID: [{}] Could not perform deletion.", flowId, e);
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

	private RangeQueryBuilder buildRelativeRangeQuery(int relativeMinutesFrom) {
		return buildRelativeRangeQuery(relativeMinutesFrom, 0);
	}

    private RangeQueryBuilder buildRelativeRangeQuery(int relativeMinutesFrom, int relativeMinutesTo) {
		return QueryBuilders.rangeQuery("meta.taskBegin").from(buildElasticRelativeTime(relativeMinutesFrom)).to(relativeMinutesTo == 0 ? "now" : buildElasticRelativeTime(relativeMinutesTo));
	}

	private String buildElasticRelativeTime(int minutes) {
		return "now-"+ minutes + "m";
	}

	private Set<String> getIndexedEnvs() {
		return ElasticsearchUtil.getEnvSet();
	}

	public Bulker getBulker() {
		return bulker;
	}

	public IndexRetryManager getRetryManager() {
		return retryManager;
	}
}


package com.datorama.oss.timbermill;

import java.io.IOException;
import java.io.InputStream;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.*;
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
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.util.IOUtils;
import com.datorama.oss.timbermill.common.Constants;
import com.datorama.oss.timbermill.common.ElasticsearchUtil;
import com.datorama.oss.timbermill.common.disk.DbBulkRequest;
import com.datorama.oss.timbermill.common.disk.DiskHandler;
import com.datorama.oss.timbermill.common.disk.IndexRetryManager;
import com.datorama.oss.timbermill.unit.Task;
import com.datorama.oss.timbermill.unit.TaskStatus;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.internal.LazilyParsedNumber;

import kamon.Kamon;
import kamon.metric.Metric;
import static com.datorama.oss.timbermill.common.ElasticsearchUtil.TIMBERMILL_INDEX_PREFIX;
import static org.elasticsearch.action.update.UpdateHelper.ContextFields.CTX;
import static org.elasticsearch.common.Strings.EMPTY_ARRAY;

public class ElasticsearchClient {

	public static final TermsQueryBuilder PARTIALS_QUERY = new TermsQueryBuilder("status", TaskStatus.PARTIAL_ERROR, TaskStatus.PARTIAL_INFO_ONLY, TaskStatus.PARTIAL_SUCCESS, TaskStatus.UNTERMINATED);
	private static final TermQueryBuilder ORPHANS_QUERY = QueryBuilders.termQuery("orphan", true);
	public static final String[] ALL_TASK_FIELDS = {"*"};

	private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchClient.class);
	private Metric.Histogram tasksFetchedFromDiskHistogram = Kamon.histogram("timbermill2.tasks.fetched.from.disk.histogram");
	private static final String TTL_FIELD = "meta.dateToDelete";
	private static final String[] PARENT_FIELD_TO_FETCH = { "env", "parentId", "orphan", "primaryId", CTX + ".*", "parentsPath", "name"};
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

	private final ReadWriteLock indexRolloverValuesAccessController;
	private final Metric.Counter partialTasksFetchedCounter= Kamon.counter("timbermill2.partial.tasks.fetched.counter");
	private final Metric.Counter partialTasksMigratedCounter= Kamon.counter("timbermill2.partial.tasks.migrated.counter");

	public ElasticsearchClient(String elasticUrl, int indexBulkSize, int indexingThreads, String awsRegion, String elasticUser, String elasticPassword, long maxIndexAge,
			long maxIndexSizeInGB, long maxIndexDocs, int numOfElasticSearchActionsTries, int maxBulkIndexFetches, int searchMaxSize, DiskHandler diskHandler, int numberOfShards, int numberOfReplicas,
			int maxTotalFields, Bulker bulker) {

		if (diskHandler!=null && !diskHandler.isCreatedSuccessfully()){
			diskHandler = null;
		}

		validateProperties(indexBulkSize, indexingThreads, maxIndexAge, maxIndexSizeInGB, maxIndexDocs, numOfElasticSearchActionsTries, numOfElasticSearchActionsTries);
		this.indexBulkSize = indexBulkSize;
		this.searchMaxSize = searchMaxSize;
		this.maxIndexAge = maxIndexAge;
        this.maxIndexSizeInGB = maxIndexSizeInGB;
        this.maxIndexDocs = maxIndexDocs;
		this.numOfElasticSearchActionsTries = numOfElasticSearchActionsTries;
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
        if (bulker == null){
        	bulker = new Bulker(client);
		}
        this.bulker = bulker;
		this.retryManager = new IndexRetryManager(numOfElasticSearchActionsTries, maxBulkIndexFetches, diskHandler, bulker, tasksFetchedFromDiskHistogram);
		bootstrapElasticsearch(numberOfShards, numberOfReplicas, maxTotalFields);
		this.indexRolloverValuesAccessController = new ReentrantReadWriteLock();
    }

    public Map<String, Task> getMissingParents(Set<String> startEventsIds, Set<String> parentIds) {
        parentIds.removeAll(startEventsIds);
        LOG.info("Fetching {} missing parents", parentIds.size());
        Map<String, Task> previouslyIndexedParentTasks = Maps.newHashMap();
        try {
            previouslyIndexedParentTasks = fetchIndexedTasks(parentIds);
        } catch (Throwable t) {
            LOG.error("Error fetching indexed tasks from Elasticsearch", t);
        }
        LOG.info("Fetched {} parents", previouslyIndexedParentTasks.size());
        return previouslyIndexedParentTasks;
    }

	private void validateProperties(int indexBulkSize, int indexingThreads, long maxIndexAge, long maxIndexSizeInGB, long maxIndexDocs, int numOfMergedTasksTries, int numOfElasticSearchActionsTries) {
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

    private Map<String, Task> fetchIndexedTasks(Set<String> tasksToFetch) {
		Map<String, Task> fetchedTasks = Collections.emptyMap();
		if (!tasksToFetch.isEmpty()) {
			fetchedTasks = getNonOrphansTasksByIds(tasksToFetch);
			for (String taskId : tasksToFetch) {
				if (!fetchedTasks.containsKey(taskId)){
					LOG.debug("Couldn't find missing parent task with ID {} in Elasticsearch", taskId);
				}
			}
		}
		return fetchedTasks;
	}

    public Task getTaskById(String taskId){
		Map<String, Task> tasksByIds = getTasksByIds(null, Sets.newHashSet(taskId), "Test", ALL_TASK_FIELDS, EMPTY_ARRAY);
		return tasksByIds.get(taskId);
	}

    public List<Task> getMultipleTasksByIds(String taskId) {
        IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery().addIds(taskId);
		Map<String, List<Task>> map = runScrollQuery(null, idsQueryBuilder, "Test", EMPTY_ARRAY, ALL_TASK_FIELDS);
		return map.get(taskId);
    }

    public Map<String, Task> getTasksByIds(String index, Set<String> taskIds, String functionDescription, String[] taskFieldsToInclude, String[] taskFieldsToExclude) {
        IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery();
        for (String taskId : taskIds) {
            idsQueryBuilder.addIds(taskId);
        }
		return getSingleTaskByIds(idsQueryBuilder, index, functionDescription, taskFieldsToInclude, taskFieldsToExclude);
    }

	private Map<String, Task> getNonOrphansTasksByIds(Set<String> taskIds) {
		IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery();
		for (String taskId : taskIds) {
			idsQueryBuilder.addIds(taskId);
		}
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		ExistsQueryBuilder startedTaskQueryBuilder = QueryBuilders.existsQuery("primaryId");

		boolQueryBuilder.filter(idsQueryBuilder);
		boolQueryBuilder.filter(startedTaskQueryBuilder);
		boolQueryBuilder.mustNot(ORPHANS_QUERY);
		return getSingleTaskByIds(boolQueryBuilder, null, "Fetch previously indexed parent tasks", ElasticsearchClient.PARENT_FIELD_TO_FETCH, EMPTY_ARRAY);
	}

	public Map<String, Task> getLatestOrphanIndexed(ZonedDateTime from) {
		Set<String> envsToFilterOn = getIndexedEnvs();
		BoolQueryBuilder finalOrphansQuery = QueryBuilders.boolQuery();
		RangeQueryBuilder partialOrphansRangeQuery = buildRangeQuerySince(from);
		TermsQueryBuilder envsQuery = QueryBuilders.termsQuery("env", envsToFilterOn);

		BoolQueryBuilder orphansWithoutPartialLimitationQuery = QueryBuilders.boolQuery();
		orphansWithoutPartialLimitationQuery.mustNot(partialOrphansRangeQuery);

		BoolQueryBuilder nonPartialOrphansQuery = QueryBuilders.boolQuery();
		nonPartialOrphansQuery.mustNot(PARTIALS_QUERY);
		nonPartialOrphansQuery.must(partialOrphansRangeQuery);

		finalOrphansQuery.filter(ORPHANS_QUERY);
		finalOrphansQuery.filter(envsQuery);
		finalOrphansQuery.should(orphansWithoutPartialLimitationQuery);
		finalOrphansQuery.should(nonPartialOrphansQuery);

		return getSingleTaskByIds(finalOrphansQuery, null, "Fetch latest indexed orphans", PARENT_FIELD_TO_FETCH, EMPTY_ARRAY);

	}

    public Map<String, Task> getSingleTaskByIds(AbstractQueryBuilder queryBuilder, String index, String functionDescription, String[] taskFieldsToInclude, String[] taskFieldsToExclude) {
        Map<String, Task> retMap = Maps.newHashMap();
		Map<String, List<Task>> tasks = runScrollQuery(index, queryBuilder, functionDescription, taskFieldsToInclude, taskFieldsToExclude);
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

    void indexMetaDataTasks(String env, Collection<String> metadataEvents) {
        String index = createTimbermillAlias(env);

        BulkRequest bulkRequest = new BulkRequest();
        for (String metadataEvent : metadataEvents) {
            IndexRequest indexRequest = new IndexRequest(index, Constants.TYPE).source(metadataEvent, XContentType.JSON);
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

	// return true iff execution of bulk finished successfully
	public boolean sendDbBulkRequest(DbBulkRequest dbBulkRequest) {
		BulkRequest request = dbBulkRequest.getRequest();
		int numberOfActions = request.numberOfActions();
		LOG.debug("Batch of {} index requests sent to Elasticsearch. Batch size: {} bytes", numberOfActions, request.estimatedSizeInBytes());

		try {
			BulkResponse responses = bulk(dbBulkRequest);
			if (responses.hasFailures()) {
				return retryManager.retrySendDbBulkRequest(dbBulkRequest,responses,responses.buildFailureMessage());
			}
			LOG.debug("Batch of {} index requests finished successfully. Took: {} millis.", numberOfActions, responses.getTook().millis());
			if (dbBulkRequest.getTimesFetched() > 0 ){
				tasksFetchedFromDiskHistogram.withTag("outcome","success").record(1);
			}
			return true;
		} catch (Throwable t) {
			return retryManager.retrySendDbBulkRequest(dbBulkRequest,null,t.getMessage());
		}
	}

	// wrap bulk method as a not-final method in order that Mockito will able to mock it
	BulkResponse bulk(DbBulkRequest request) throws IOException {
		return bulker.bulk(request);
	}

	public void index(Map<String, Task> tasksMap, String index) {
        Collection<Pair<Future, DbBulkRequest>> futuresRequests = createFuturesRequests(tasksMap, index);

        for (Pair<Future, DbBulkRequest> futureRequest : futuresRequests) {
			try {
				futureRequest.getLeft().get();
			} catch (InterruptedException e) {
				LOG.error("An error was thrown while indexing a batch, going to retry", e);
				sendDbBulkRequest(futureRequest.getRight());
			} catch (ExecutionException e) {
				LOG.error("An error was thrown while indexing a batch, which won't be persisted to disk", e);
			}
        }
    }

    void rolloverIndex(String timbermillAlias) {
		try {
			RolloverRequest rolloverRequest = new RolloverRequest(timbermillAlias, null);
			rolloverRequest.addMaxIndexAgeCondition(new TimeValue(maxIndexAge, TimeUnit.DAYS));
			rolloverRequest.addMaxIndexSizeCondition(new ByteSizeValue(maxIndexSizeInGB, ByteSizeUnit.GB));
			rolloverRequest.addMaxIndexDocsCondition(maxIndexDocs);
			RolloverResponse rolloverResponse = (RolloverResponse) runWithRetries(() -> client.indices().rollover(rolloverRequest, RequestOptions.DEFAULT), 1, "Rollover alias " + timbermillAlias);
			if (rolloverResponse.isRolledOver()){
				LOG.info("Index {} rolled over, new index is [{}]", rolloverResponse.getOldIndex(), rolloverResponse.getNewIndex());
				setRollOveredIndicesValues(rolloverResponse.getOldIndex(), rolloverResponse.getNewIndex());

			}
		} catch (Exception e) {
			LOG.error("Could not rollovered index " + timbermillAlias);
		}
    }

	public int migrateTasksToNewIndex(ZonedDateTime fromTime) {
		Map<String, Task> tasksToMigrateIntoNewIndex = Maps.newHashMap();
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		RangeQueryBuilder latestItemsQuery = buildRangeQuerySince(fromTime);
		TermsQueryBuilder envsQuery = QueryBuilders.termsQuery("env", getIndexedEnvs());

		boolQueryBuilder.filter(latestItemsQuery);
		boolQueryBuilder.filter(envsQuery);
		boolQueryBuilder.filter(PARTIALS_QUERY);

		indexRolloverValuesAccessController.readLock().lock();
		String oldIndex = this.oldIndex;
		String currentIndex = this.currentIndex;
		indexRolloverValuesAccessController.readLock().unlock();

		tasksToMigrateIntoNewIndex.putAll(findMatchingTasksToMigrate(boolQueryBuilder, oldIndex, currentIndex));
		tasksToMigrateIntoNewIndex.putAll(findMatchingTasksToMigrate(boolQueryBuilder, currentIndex, oldIndex));
		indexAndDeleteTasks(tasksToMigrateIntoNewIndex, oldIndex, currentIndex);
		partialTasksMigratedCounter.withoutTags().increment(tasksToMigrateIntoNewIndex.size());

		return tasksToMigrateIntoNewIndex.size();
	}

	private Map<String,Task> findMatchingTasksToMigrate(BoolQueryBuilder queryBuilder, String indexOfPartials, String indexOfMatchingTasks) {
		Map<String, Task> previousIndexMatchingTasks = Maps.newHashMap();
		String functionDescription = "Migrate old tasks to new index";
		Map<String, Task> singleTaskByIds = getSingleTaskByIds(queryBuilder, indexOfPartials, functionDescription, EMPTY_ARRAY, ALL_TASK_FIELDS);
		if (!singleTaskByIds.isEmpty()) {
			partialTasksFetchedCounter.withoutTags().increment(singleTaskByIds.size());
			previousIndexMatchingTasks = getTasksByIds(indexOfMatchingTasks, singleTaskByIds.keySet(), functionDescription, ALL_TASK_FIELDS, EMPTY_ARRAY);
		}
		return previousIndexMatchingTasks;
	}

	private void indexAndDeleteTasks(Map<String, Task> previousIndexPartialTasks, String oldIndex, String currentIndex) {
		if (!previousIndexPartialTasks.isEmpty()) {
			LOG.info("Starting migration between old index [{}] and new index [{}]", oldIndex, currentIndex);
			index(previousIndexPartialTasks, currentIndex);
			deleteTasksFromIndex(previousIndexPartialTasks.keySet(), oldIndex);
			LOG.info("Successfully migrated {} tasks to new index [{}]", previousIndexPartialTasks.size(), currentIndex);
		}
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
        Future<?> future = executorService.submit(() -> sendDbBulkRequest(request));
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
		request.id(Constants.TIMBERMILL_SCRIPT);
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

        request.patterns(Lists.newArrayList(TIMBERMILL_INDEX_PREFIX + "*"));
        request.settings(Settings.builder().put("index.mapping.total_fields.limit", maxTotalFields)
				.put("number_of_shards", numberOfShards)
				.put("number_of_replicas", numberOfReplicas));
		request.mapping(ElasticsearchUtil.MAPPING, XContentType.JSON);
		runWithRetries(() -> client.indices().putTemplate(request, RequestOptions.DEFAULT), 1, "Put Timbermill Index Template");
    }

    public String createTimbermillAlias(String env) {
        String timbermillAlias = ElasticsearchUtil.getTimbermillIndexAlias(env);
		String initialIndex = getInitialIndex(timbermillAlias);
		GetAliasesRequest requestWithAlias = new GetAliasesRequest(timbermillAlias);
		try {
			GetAliasesResponse response = (GetAliasesResponse) runWithRetries(() -> client.indices().getAlias(requestWithAlias, RequestOptions.DEFAULT), 1, "Create Timbermill Alias for env " + env);
			boolean exists = !response.getAliases().isEmpty();
			if (!exists) {
				CreateIndexRequest request = new CreateIndexRequest(initialIndex);
				Alias alias = new Alias(timbermillAlias);
				request.alias(alias);
				runWithRetries(() -> client.indices().create(request, RequestOptions.DEFAULT), 1, "Create index alias " + timbermillAlias + " for index " + initialIndex);
				setRollOveredIndicesValues(null, initialIndex);
			}
		} catch (MaxRetriesException e){
			LOG.error("Failed creating Timbermill Alias " + timbermillAlias + ", going to use index " + initialIndex);
		}
		return timbermillAlias;
	}

    private String getInitialIndex(String timbermillAlias) {
        String initialSerial = ElasticsearchUtil.getIndexSerial(1);
        return timbermillAlias + ElasticsearchUtil.INDEX_DELIMITER + initialSerial;
    }

	private Map<String, List<Task>> runScrollQuery(String index, QueryBuilder query, String functionDescription, String[] taskFieldsToInclude, String[] taskFieldsToExclude){
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
        searchSourceBuilder.fetchSource(taskFieldsToInclude, taskFieldsToExclude);
        searchSourceBuilder.query(query);
        searchSourceBuilder.size(searchMaxSize);
        searchRequest.source(searchSourceBuilder);

		List<SearchResponse> searchResponses = new ArrayList<>();
		try {
			SearchResponse searchResponse = (SearchResponse) runWithRetries(() -> client.search(searchRequest, RequestOptions.DEFAULT), 1, "Initial search for " + functionDescription);
			String scrollId = searchResponse.getScrollId();
			searchResponses.add(searchResponse);
			SearchHit[] searchHits = searchResponse.getHits().getHits();
			boolean keepScrolling = searchHits != null && searchHits.length > 0;
			while (keepScrolling) {
				SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
				scrollRequest.scroll(scroll);
				SearchResponse scrollResponse = (SearchResponse) runWithRetries(() -> client.scroll(scrollRequest, RequestOptions.DEFAULT), 1, "Scroll search for scroll id: " + scrollId + " for " + functionDescription);
				scrollId = scrollResponse.getScrollId();
				searchResponses.add(scrollResponse);
				SearchHit[] scrollHits = scrollResponse.getHits().getHits();
				keepScrolling = scrollHits != null && scrollHits.length > 0;
			}
			ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
			clearScrollRequest.addScrollId(scrollId);
			try {
				ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
				boolean succeeded = clearScrollResponse.isSucceeded();
				if (!succeeded) {
					LOG.error("Couldn't clear scroll id " + scrollId + " for " + functionDescription  +" in index " + index);
				}
			} catch (Throwable e) {
				LOG.error("Couldn't clear scroll id " + scrollId + " for " + functionDescription  +" in index " + index, e);
			}
		}
		catch (MaxRetriesException e) {
			LOG.error("Error while running search query.", e);
		}
		return addHitsToMap(searchResponses);
    }

	private ActionResponse runWithRetries(Callable<ActionResponse> callable, int tryNum, String functionDescription) throws MaxRetriesException {
		if (tryNum > 1) {
			LOG.info("Retry # {}/{} for [{}]", tryNum, numOfElasticSearchActionsTries, functionDescription);
		}
		try {
			return callable.call();
		} catch (Exception e) {
			if (tryNum <= numOfElasticSearchActionsTries){
				double sleep = Math.pow(2, tryNum);
				LOG.warn("Retry # " + tryNum + "/" + numOfElasticSearchActionsTries + " for [" + functionDescription + "] failed. Going to sleep " + sleep + " seconds.", e);
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
        LOG.info("Deleting tasks from  index [{}]", index);
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
		deleteByQuery("*", query);
    }

    private void deleteByQuery(String index, String query) {
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

    private RangeQueryBuilder buildRangeQuerySince(ZonedDateTime zonedDateTime) {
		return QueryBuilders.rangeQuery("meta.taskBegin").from(zonedDateTime).to("now").timeZone(zonedDateTime.getZone().toString());
	}

	public Set<String> getIndexedEnvs() {
		return ElasticsearchUtil.getEnvSet();
	}

	public Bulker getBulker() {
		return bulker;
	}

	public IndexRetryManager getRetryManager() {
		return retryManager;
	}
}


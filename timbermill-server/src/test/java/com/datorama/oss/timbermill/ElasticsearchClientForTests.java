package com.datorama.oss.timbermill;

import java.io.IOException;
import java.io.InputStream;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
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
import org.elasticsearch.client.indices.GetIndexRequest;
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
import org.elasticsearch.search.slice.SliceBuilder;
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

import static com.datorama.oss.timbermill.TaskIndexer.FLOW_ID_LOG;
import static com.datorama.oss.timbermill.common.ElasticsearchUtil.*;
import static org.elasticsearch.action.update.UpdateHelper.ContextFields.CTX;
import static org.elasticsearch.common.Strings.EMPTY_ARRAY;

public class ElasticsearchClientForTests extends ElasticsearchClient{

	public ElasticsearchClientForTests(String elasticUrl, String awsRegion) {
			super(elasticUrl, 1000, 1, awsRegion, null, null,
					7, 100, 1000000000,3, 3, 1000,null ,1, 1,
					4000, null, 10 , 60, 10000, 10);
    }

	public void createTimbermillAliasForMigrationTest(String currentIndex, String oldIndex, String env) throws IOException {
		IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
		String alias = getTimbermillIndexAlias(env);
		String oldAlias = getOldAlias(alias);
		IndicesAliasesRequest.AliasActions addOldIndexAction = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD).index(oldIndex).alias(oldAlias);
		IndicesAliasesRequest.AliasActions addNewIndexAction = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD).index(currentIndex).alias(alias);
		indicesAliasesRequest.addAliasAction(addOldIndexAction);
		indicesAliasesRequest.addAliasAction(addNewIndexAction);
		client.indices().updateAliases(indicesAliasesRequest, RequestOptions.DEFAULT);
	}

	public void createTimbermillIndexForTests(String index) throws IOException {
		GetIndexRequest exists = new GetIndexRequest(index);
		boolean isExists = client.indices().exists(exists, RequestOptions.DEFAULT);
		if (!isExists) {
			CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
			client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
		}
	}

}


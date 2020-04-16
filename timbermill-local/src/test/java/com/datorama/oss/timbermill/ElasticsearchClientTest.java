package com.datorama.oss.timbermill;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.datorama.oss.timbermill.common.Constants;
import com.datorama.oss.timbermill.common.DbBulkRequest;

import static com.datorama.oss.timbermill.common.Constants.DEFAULT_ELASTICSEARCH_URL;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

@RunWith(MockitoJUnitRunner.class)
public class ElasticsearchClientTest {


	private static ElasticsearchClient elasticsearchClient;

	@BeforeClass
	public static void init() {
		String elasticUrl = System.getenv("ELASTICSEARCH_URL");
		if (StringUtils.isEmpty(elasticUrl)) {
			elasticUrl = DEFAULT_ELASTICSEARCH_URL;
		}
		elasticsearchClient = new ElasticsearchClient(elasticUrl, 1000, 1, null, null, null,
				7, 100, 1000000000, 3, 3, 3, null, null);
	}

	@Test
	public void failAllOfRequestsOfBulk() throws IOException {
		int amountOfRequestsInBulk = 5;
		DbBulkRequest bulkRequest = createMockDbBulkRequest(amountOfRequestsInBulk);
		BulkResponse bulkResponse = elasticsearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);

		elasticsearchClient.handleBulkRequestFailure(bulkRequest, 0, bulkResponse, "");

		DbBulkRequest fetchedBulkRequest = elasticsearchClient.memoryFailedRequestsAsList().get(0).getKey();
		int bulkNewSize = fetchedBulkRequest.size();
		Assert.assertEquals(amountOfRequestsInBulk, bulkNewSize);
	}

	@Test
	public void failSomeOfRequestsOfBulk() throws IOException {
		int amountOfRequestsInBulk = 2;
		DbBulkRequest bulkRequest = createMockDbBulkRequest(amountOfRequestsInBulk);
		BulkResponse bulkResponse = elasticsearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);

		String successItemId = makeItemSuccess(bulkResponse,0);
		elasticsearchClient.handleBulkRequestFailure(bulkRequest, 0, bulkResponse, "");

		DbBulkRequest fetchedBulkRequest = elasticsearchClient.memoryFailedRequestsAsList().get(0).getKey();
		int bulkNewSize = fetchedBulkRequest.size();
		Assert.assertEquals(amountOfRequestsInBulk-1, bulkNewSize);
		Assert.assertNotEquals(successItemId, fetchedBulkRequest.getRequest().requests().get(0).id());
	}

	@Test
	public void successAllOfRequestsOfBulk() throws IOException {
		DbBulkRequest bulkRequest = createMockDbBulkRequest(2);
		BulkResponse bulkResponse = elasticsearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);

		makeItemSuccess(bulkResponse,0);
		makeItemSuccess(bulkResponse,1);
		elasticsearchClient.handleBulkRequestFailure(bulkRequest, 0, bulkResponse, "");

		DbBulkRequest fetchedBulkRequest = elasticsearchClient.memoryFailedRequestsAsList().get(0).getKey();
		int bulkNewSize = fetchedBulkRequest.size();
		Assert.assertEquals(0, bulkNewSize);
	}

	@AfterClass
	public static void tearDown(){
		elasticsearchClient.close();
	}

	// make bulk's item #itemNumber to not fail
	private String makeItemSuccess(BulkResponse bulkResponse,int itemNumber) {
		BulkItemResponse spy = spy(bulkResponse.getItems()[itemNumber]);
		bulkResponse.getItems()[itemNumber]=spy;
		doReturn(false).when(spy).isFailed();
		return spy.getId();
	}

	// mock request always fails
	private UpdateRequest createMockRequest() {
		String taskId = UUID.randomUUID().toString();
		String index = "timbermill-test";
		UpdateRequest updateRequest = new UpdateRequest(index, Constants.TYPE, taskId);
		Script script = new Script(ScriptType.STORED, null, Constants.TIMBERMILL_SCRIPT, new HashMap<>());
		updateRequest.script(script);
		return updateRequest;
	}

	private DbBulkRequest createMockDbBulkRequest(int amountOfRequestsInBulk) {
		BulkRequest bulkRequest = new BulkRequest();
		for (int i = 0 ; i < amountOfRequestsInBulk ; i++){
			bulkRequest.add(createMockRequest());
		}
		return new DbBulkRequest(bulkRequest);
	}
}
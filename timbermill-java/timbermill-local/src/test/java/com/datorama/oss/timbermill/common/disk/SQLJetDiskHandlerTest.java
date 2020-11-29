package com.datorama.oss.timbermill.common.disk;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.junit.*;

import com.datorama.oss.timbermill.ElasticsearchClient;
import com.datorama.oss.timbermill.common.exceptions.MaximumInsertTriesException;
import org.tmatesoft.sqljet.core.SqlJetException;

import static org.junit.Assert.*;

public class SQLJetDiskHandlerTest {

	private static SQLJetDiskHandler sqlJetDiskHandler;
	private static int maxFetchedBulks = 10;
	private static String flowId = "test";
	private int bulkNum = 1;

	@BeforeClass
	public static void init()  {
		sqlJetDiskHandler = new SQLJetDiskHandler(maxFetchedBulks, 3,"/tmp/SQLJetDiskHandlerTest");
	}

	@Before
	public void emptyDbBeforeTest() {
		sqlJetDiskHandler.reset();
	}

	@After
	public void tearDown(){
		sqlJetDiskHandler.reset();
	}

	@Test
	public void hasFailedBulks() throws MaximumInsertTriesException {
		DbBulkRequest dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
		sqlJetDiskHandler.persistBulkRequestToDisk(dbBulkRequest, flowId, bulkNum);
		assertTrue(sqlJetDiskHandler.hasFailedBulks(flowId));
	}

	@Test
	public void fetchFailedBulksAdvanced() throws MaximumInsertTriesException {

		DbBulkRequest dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
		sqlJetDiskHandler.persistBulkRequestToDisk(dbBulkRequest, flowId, bulkNum);
		List<DbBulkRequest> fetchedRequests = sqlJetDiskHandler.fetchFailedBulks(false, flowId);
		assertEquals(1, fetchedRequests.size());

		DbBulkRequest dbBulkRequestFromDisk = fetchedRequests.get(0);
		assertEquals(getRequestAsString(dbBulkRequest), getRequestAsString(dbBulkRequestFromDisk));

		DbBulkRequest dbBulkRequest2 = MockBulkRequest.createMockDbBulkRequest();
		sqlJetDiskHandler.persistBulkRequestToDisk(dbBulkRequest2, flowId, bulkNum);
		assertEquals(2, sqlJetDiskHandler.fetchAndDeleteFailedBulks(flowId).size());
		assertFalse(sqlJetDiskHandler.hasFailedBulks(flowId));
	}

	@Test
	public void fetchTimesCounter() throws MaximumInsertTriesException {
		DbBulkRequest dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
		sqlJetDiskHandler.persistBulkRequestToDisk(dbBulkRequest, flowId, bulkNum);
		DbBulkRequest fetchedRequest = sqlJetDiskHandler.fetchAndDeleteFailedBulks(flowId).get(0);
		sqlJetDiskHandler.persistBulkRequestToDisk(fetchedRequest, flowId, bulkNum);
		fetchedRequest= sqlJetDiskHandler.fetchFailedBulks(false, flowId).get(0);
		assertEquals(2, fetchedRequest.getTimesFetched());
	}

	@Test
	public void failedBulksAmount() throws MaximumInsertTriesException {
		DbBulkRequest dbBulkRequest;
		int amount = 3;
		for (int i = 0 ; i < amount ; i++){
			dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
			sqlJetDiskHandler.persistBulkRequestToDisk(dbBulkRequest, flowId, bulkNum);
		}
		assertEquals(3, sqlJetDiskHandler.failedBulksAmount());
		assertEquals(3, sqlJetDiskHandler.failedBulksAmount()); // to make sure the db didn't change after the call to failedBulksAmount
	}

	@Test
	public void failToInsert() {
		boolean thrown = false;
		DbBulkRequest dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
		dbBulkRequest.setRequest(null); // will cause insert to fail
		try {
			sqlJetDiskHandler.persistBulkRequestToDisk(dbBulkRequest,0, flowId, bulkNum);
		} catch (MaximumInsertTriesException e){
			thrown = true;
		}
		assertTrue(thrown);
	}

	@Test
	public void persistManyBulks() throws MaximumInsertTriesException {
		DbBulkRequest dbBulkRequest;
		int extraBulks = 2;
		for (int i = 0 ; i < maxFetchedBulks + extraBulks ; i++){
			dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
			dbBulkRequest.setId(i+1);
			sqlJetDiskHandler.persistBulkRequestToDisk(dbBulkRequest, flowId, bulkNum);
		}
		List<DbBulkRequest> fetchedRequests = sqlJetDiskHandler.fetchAndDeleteFailedBulks(flowId);
		assertEquals(maxFetchedBulks,fetchedRequests.size());
		assertEquals(extraBulks, sqlJetDiskHandler.failedBulksAmount());
	}

	@Test
	public void dropAndRecreateTable() throws MaximumInsertTriesException {
		DbBulkRequest dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
		sqlJetDiskHandler.persistBulkRequestToDisk(dbBulkRequest, flowId, bulkNum);

		sqlJetDiskHandler.reset();
		assertFalse(sqlJetDiskHandler.hasFailedBulks(flowId));
	}

	@Ignore
	@Test
	public void testMultiThreadSafety() throws MaximumInsertTriesException, InterruptedException {

		AtomicBoolean isHealthCheckFailed = new AtomicBoolean(false);

		for (int i = 0 ; i < 10 ; i++){
			DbBulkRequest dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
			sqlJetDiskHandler.persistBulkRequestToDisk(dbBulkRequest, flowId, bulkNum);
		}

		int numOfThreads = 30;
		ExecutorService executorService = Executors.newFixedThreadPool(numOfThreads);

		Runnable fetchAndPersistTask = () -> {
			while (true) {

				try {
					sqlJetDiskHandler.healthCheck();
				} catch (SqlJetException e) {
					isHealthCheckFailed.set(true);
					e.printStackTrace();
					break;
				}

				fetchAndPersist();
			}
		};

		for (int i = 0; i < numOfThreads; i++) {
			executorService.execute(fetchAndPersistTask);
		}
		Thread.sleep(2000);
		assertFalse(isHealthCheckFailed.get());
	}

	// region Test Helpers
	private void fetchAndPersist() {
        if (sqlJetDiskHandler.hasFailedBulks(flowId)) {
            List<DbBulkRequest> failedRequestsFromDisk = sqlJetDiskHandler.fetchAndDeleteFailedBulks(flowId);
            if (failedRequestsFromDisk.size() == 0) {
                return;
            }
            for (DbBulkRequest dbBulkRequest : failedRequestsFromDisk) {
                try {
                    sqlJetDiskHandler.persistBulkRequestToDisk(dbBulkRequest, flowId, bulkNum);
                } catch (MaximumInsertTriesException e) {
                    e.printStackTrace();
                }
            }
        }
    }

	private String getRequestAsString(DbBulkRequest dbBulkRequest) {
		return dbBulkRequest.getRequest().requests().get(0).toString();
	}

	public static class MockBulkRequest {
		static UpdateRequest createMockRequest() {
			String taskId = UUID.randomUUID().toString();
			String index = "timbermill-test";
			UpdateRequest updateRequest = new UpdateRequest(index, ElasticsearchClient.TYPE, taskId);
			Script script = new Script(ScriptType.STORED, null, ElasticsearchClient.TIMBERMILL_SCRIPT, new HashMap<>());
			updateRequest.script(script);
			return updateRequest;
		}

		static DbBulkRequest createMockDbBulkRequest() {
			BulkRequest bulkRequest = new BulkRequest();
			for (int i = 0 ; i < 3 ; i++){
				bulkRequest.add(createMockRequest());
			}
			return new DbBulkRequest(bulkRequest);
		}
	}

	// endregion

}


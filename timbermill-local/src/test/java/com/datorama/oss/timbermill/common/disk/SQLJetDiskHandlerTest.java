package com.datorama.oss.timbermill.common.disk;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.junit.*;

import com.datorama.oss.timbermill.common.Constants;
import com.datorama.oss.timbermill.common.exceptions.MaximumInsertTriesException;

import static org.junit.Assert.*;

public class SQLJetDiskHandlerTest {

	private static SQLJetDiskHandler diskHandler;
	private static int maxFetchedBulks = 10;
	private static String flowId = "test";
	private int bulkNum = 1;

	@BeforeClass
	public static void init()  {
		diskHandler = new SQLJetDiskHandler(maxFetchedBulks, 3,"/tmp/SQLJetDiskHandlerTest");
	}

	@Before
	public void emptyDbBeforeTest() {
		diskHandler.close();
	}

	@After
	public void tearDown(){
		diskHandler.close();
	}

	@Test
	public void hasFailedBulks() throws MaximumInsertTriesException {
		DbBulkRequest dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
		diskHandler.persistBulkRequestToDisk(dbBulkRequest, flowId, bulkNum);
		assertTrue(diskHandler.hasFailedBulks(flowId));
	}

	@Test
	public void fetchFailedBulksAdvanced() throws MaximumInsertTriesException {

		DbBulkRequest dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
		diskHandler.persistBulkRequestToDisk(dbBulkRequest, flowId, bulkNum);
		List<DbBulkRequest> fetchedRequests = diskHandler.fetchFailedBulks(false, flowId);
		assertEquals(1, fetchedRequests.size());

		DbBulkRequest dbBulkRequestFromDisk = fetchedRequests.get(0);
		assertEquals(getRequestAsString(dbBulkRequest), getRequestAsString(dbBulkRequestFromDisk));

		DbBulkRequest dbBulkRequest2 = MockBulkRequest.createMockDbBulkRequest();
		diskHandler.persistBulkRequestToDisk(dbBulkRequest2, flowId, bulkNum);
		assertEquals(2, diskHandler.fetchAndDeleteFailedBulks(flowId).size());
		assertFalse(diskHandler.hasFailedBulks(flowId));
	}

	@Test
	public void fetchTimesCounter() throws MaximumInsertTriesException {
		DbBulkRequest dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
		diskHandler.persistBulkRequestToDisk(dbBulkRequest, flowId, bulkNum);
		DbBulkRequest fetchedRequest = diskHandler.fetchAndDeleteFailedBulks(flowId).get(0);
		diskHandler.persistBulkRequestToDisk(fetchedRequest, flowId, bulkNum);
		fetchedRequest=diskHandler.fetchFailedBulks(false, flowId).get(0);
		assertEquals(2, fetchedRequest.getTimesFetched());
	}

	@Test
	public void failedBulksAmount() throws MaximumInsertTriesException {
		DbBulkRequest dbBulkRequest;
		int amount = 3;
		for (int i = 0 ; i < amount ; i++){
			dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
			diskHandler.persistBulkRequestToDisk(dbBulkRequest, flowId, bulkNum);
		}
		assertEquals(3, diskHandler.failedBulksAmount());
		assertEquals(3, diskHandler.failedBulksAmount()); // to make sure the db didn't change after the call to failedBulksAmount
	}

	@Test
	public void failToInsert() {
		boolean thrown = false;
		DbBulkRequest dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
		dbBulkRequest.setRequest(null); // will cause insert to fail
		try {
			diskHandler.persistBulkRequestToDisk(dbBulkRequest,0, flowId, bulkNum);
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
			diskHandler.persistBulkRequestToDisk(dbBulkRequest, flowId, bulkNum);
		}
		List<DbBulkRequest> fetchedRequests = diskHandler.fetchAndDeleteFailedBulks(flowId);
		assertEquals(maxFetchedBulks,fetchedRequests.size());
		assertEquals(extraBulks,diskHandler.failedBulksAmount());
	}

	@Test
	public void dropAndRecreateTable() throws MaximumInsertTriesException {
		DbBulkRequest dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
		diskHandler.persistBulkRequestToDisk(dbBulkRequest, flowId, bulkNum);

		diskHandler.close();
		assertFalse(diskHandler.hasFailedBulks(flowId));
	}

	private String getRequestAsString(DbBulkRequest dbBulkRequest) {
		return dbBulkRequest.getRequest().requests().get(0).toString();
	}

	public static class MockBulkRequest {
		static UpdateRequest createMockRequest() {
			String taskId = UUID.randomUUID().toString();
			String index = "timbermill-test";
			UpdateRequest updateRequest = new UpdateRequest(index, Constants.TYPE, taskId);
			Script script = new Script(ScriptType.STORED, null, Constants.TIMBERMILL_SCRIPT, new HashMap<>());
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
}


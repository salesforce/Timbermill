package com.datorama.oss.timbermill.common;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datorama.oss.timbermill.common.exceptions.MaximunInsertTriesException;

import static org.junit.Assert.*;

public class SQLJetDiskHandlerTest {

	private static SQLJetDiskHandler diskHandler;
	public static int numOfMinutes = 1;
	public static int maxFetchedBulks = 10;
	public static int maxInsertTries = 3;


	@BeforeClass
	public static void init()  {
		diskHandler = new SQLJetDiskHandler(numOfMinutes,maxFetchedBulks, maxInsertTries,"/tmp/SQLJetDiskHandlerTest");
	}

	@Before
	public void emptyDbBeforeTest() {
		diskHandler.dropAndRecreateTable();
	}

	@AfterClass
	public static void tearDown(){
		diskHandler.dropAndRecreateTable();
	}

	@Test
	public void fetchBeforeWaitingTime() throws MaximunInsertTriesException {
		DbBulkRequest dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
		diskHandler.persistToDisk(dbBulkRequest);
		assertEquals(false, diskHandler.hasFailedBulks());
	}

	@Test
	public void fetchAfterWaitingTime() throws MaximunInsertTriesException {
		DbBulkRequest dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
		diskHandler.persistToDisk(dbBulkRequest);

		dbBulkRequest.setId(1);
		updateInsertTimeforTest(dbBulkRequest);

		assertEquals(true, diskHandler.hasFailedBulks());
	}

	@Test
	public void fetchFailedBulksAdvanced() throws MaximunInsertTriesException {

		DbBulkRequest dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
		diskHandler.persistToDisk(dbBulkRequest);

		dbBulkRequest.setId(1);
		updateInsertTimeforTest(dbBulkRequest);
		List<DbBulkRequest> fetchedRequests = diskHandler.fetchFailedBulks(false);
		assertEquals(1, fetchedRequests.size());

		DbBulkRequest dbBulkRequestFromDisk = fetchedRequests.get(0);
		assertEquals(dbBulkRequest.getId(), dbBulkRequestFromDisk.getId());
		assertEquals(getRequestAsString(dbBulkRequest), getRequestAsString(dbBulkRequestFromDisk));

		DbBulkRequest dbBulkRequest2 = MockBulkRequest.createMockDbBulkRequest();
		diskHandler.persistToDisk(dbBulkRequest2);
		dbBulkRequest2.setId(2);
		updateInsertTimeforTest(dbBulkRequest2);
		assertEquals(2, diskHandler.fetchAndDeleteFailedBulks().size());
		assertEquals(false, diskHandler.hasFailedBulks());
	}

	@Test
	public void fetchTimesCounter() throws MaximunInsertTriesException {
		DbBulkRequest dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
		diskHandler.persistToDisk(dbBulkRequest);
		dbBulkRequest.setId(1);
		updateInsertTimeforTest(dbBulkRequest);
		DbBulkRequest fetchedRequest = diskHandler.fetchAndDeleteFailedBulks().get(0);
		diskHandler.persistToDisk(fetchedRequest);
		updateInsertTimeforTest(fetchedRequest);
		fetchedRequest=diskHandler.fetchFailedBulks(false).get(0);
		assertEquals(2, fetchedRequest.getTimesFetched());
	}

	@Test
	public void updateBulk() throws MaximunInsertTriesException {
		DbBulkRequest dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
		diskHandler.persistToDisk(dbBulkRequest);
		dbBulkRequest.setId(1);
		diskHandler.updateBulk(dbBulkRequest);

		updateInsertTimeforTest(dbBulkRequest);

		List<DbBulkRequest> fetchedRequests = diskHandler.fetchAndDeleteFailedBulks();
		DbBulkRequest dbBulkRequestFromDisk = fetchedRequests.get(0);
		assertEquals(1, dbBulkRequestFromDisk.getTimesFetched());
	}

	@Test
	public void failedBulksAmount() throws MaximunInsertTriesException {
		DbBulkRequest dbBulkRequest1 = MockBulkRequest.createMockDbBulkRequest();
		dbBulkRequest1.setId(1);
		DbBulkRequest dbBulkRequest2 = MockBulkRequest.createMockDbBulkRequest();
		dbBulkRequest2.setId(2);
		DbBulkRequest dbBulkRequest3 = MockBulkRequest.createMockDbBulkRequest();
		dbBulkRequest3.setId(3);
		diskHandler.persistToDisk(dbBulkRequest1);
		updateInsertTimeforTest(dbBulkRequest1);
		diskHandler.persistToDisk(dbBulkRequest2);
		updateInsertTimeforTest(dbBulkRequest2);
		diskHandler.persistToDisk(dbBulkRequest3);
		updateInsertTimeforTest(dbBulkRequest3);

		assertEquals(3, diskHandler.failedBulksAmount());
		assertEquals(3, diskHandler.failedBulksAmount()); // to make sure the db didn't change after the call to failedBulksAmount
	}

	@Test
	public void failToInsert() {
		boolean thrown = false;
		DbBulkRequest dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
		dbBulkRequest.setRequest(null); // will cause insert to fail
		try {
			diskHandler.persistToDisk(dbBulkRequest,0);
		} catch (MaximunInsertTriesException e){
			thrown = true;
		}
		assertEquals(true,thrown);
	}

	@Test
	public void persistManyBulks() throws MaximunInsertTriesException {
		DbBulkRequest dbBulkRequest;
		int extraBulks = 2;
		for (int i = 0 ; i < maxFetchedBulks + extraBulks ; i++){
			dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
			dbBulkRequest.setId(i+1);
			diskHandler.persistToDisk(dbBulkRequest);
			updateInsertTimeforTest(dbBulkRequest);
		}
		List<DbBulkRequest> fetchedRequests = diskHandler.fetchAndDeleteFailedBulks();
		assertEquals(maxFetchedBulks,fetchedRequests.size());
		assertEquals(extraBulks,diskHandler.failedBulksAmount());
	}

	@Test
	public void dropAndRecreateTable() throws MaximunInsertTriesException {
		DbBulkRequest dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
		diskHandler.persistToDisk(dbBulkRequest);

		diskHandler.dropAndRecreateTable();
		assertEquals(false, diskHandler.hasFailedBulks());
	}

	private String getRequestAsString(DbBulkRequest dbBulkRequest) {
		return dbBulkRequest.getRequest().requests().get(0).toString();
	}

	private void updateInsertTimeforTest(DbBulkRequest dbBulkRequest) {
		dbBulkRequest.setInsertTime(DateTime.now().minusMinutes(2* numOfMinutes).toString());
		diskHandler.updateBulk(dbBulkRequest);
	}

	public static class MockBulkRequest {
		public static UpdateRequest createMockRequest() {
			String taskId = UUID.randomUUID().toString();
			String index = "timbermill-test";
			UpdateRequest updateRequest = new UpdateRequest(index, Constants.TYPE, taskId);
			Script script = new Script(ScriptType.STORED, null, Constants.TIMBERMILL_SCRIPT, new HashMap<>());
			updateRequest.script(script);
			return updateRequest;
		}

		public static DbBulkRequest createMockDbBulkRequest() {
			BulkRequest bulkRequest = new BulkRequest();
			for (int i = 0 ; i < 3 ; i++){
				bulkRequest.add(createMockRequest());
			}
			DbBulkRequest dbBulkRequest = new DbBulkRequest(bulkRequest);
			return dbBulkRequest;
		}
	}
}


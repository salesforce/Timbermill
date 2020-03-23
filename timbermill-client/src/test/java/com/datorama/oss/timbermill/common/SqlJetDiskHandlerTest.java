package com.datorama.oss.timbermill.common;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;


import static org.junit.Assert.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class SqlJetDiskHandlerTest {

	private static SQLJetDiskHandler diskHandler;
	private static int numOfMinutes = 1;
	private static int minuteInMillis = 60000;

	@BeforeClass
	public static void init()  {
		diskHandler = new SQLJetDiskHandler();
		diskHandler.setWaitingTime(numOfMinutes * minuteInMillis);
	}

	@Before
	public void emptyDbBeforeTest() {
		diskHandler.emptyDb();
	}

	@Test
	public void fetchBeforeWaitingTime()  {
		DbBulkRequest dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
		diskHandler.persistToDisk(dbBulkRequest);
		assertEquals(false, diskHandler.hasFailedBulks());
	}

	@Test
	public void fetchAfterWaitingTime()  {
		DbBulkRequest dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
		diskHandler.persistToDisk(dbBulkRequest);

		dbBulkRequest.setId(1);
		updateInsertTimeforTest(dbBulkRequest);

		assertEquals(true, diskHandler.hasFailedBulks());
	}

	@Test
	public void fetchFailedBulks()  {

		DbBulkRequest dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
		diskHandler.persistToDisk(dbBulkRequest);

		dbBulkRequest.setId(1);
		updateInsertTimeforTest(dbBulkRequest);
		List<DbBulkRequest> fetchedRequests = diskHandler.fetchFailedBulks(false);
		assertEquals(1, fetchedRequests.size());

		DbBulkRequest dbBulkRequestFromDisk = fetchedRequests.get(0);
		assertEquals(dbBulkRequest.getId(), dbBulkRequestFromDisk.getId());
		assertEquals(dbBulkRequest.getCreateTime(), dbBulkRequestFromDisk.getCreateTime());
		assertEquals(getRequestAsString(dbBulkRequest), getRequestAsString(dbBulkRequestFromDisk));

		DbBulkRequest dbBulkRequest2 = MockBulkRequest.createMockDbBulkRequest();
		diskHandler.persistToDisk(dbBulkRequest2);
		dbBulkRequest2.setId(2);
		updateInsertTimeforTest(dbBulkRequest2);
		assertEquals(2, diskHandler.failedBulksAmount());
	}

	@Test
	public void fetchTimesCounter()  {
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
	public void updateBulk() {
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
	 public void emptyDb() {
		DbBulkRequest dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
		diskHandler.persistToDisk(dbBulkRequest);
		diskHandler.emptyDb();
		assertEquals(false, diskHandler.hasFailedBulks());
	}

	@AfterClass
	public static void tearDown(){
		diskHandler.emptyDb();
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
package com.datorama.oss.timbermill.common;

import java.util.List;

import org.elasticsearch.action.DocWriteRequest;
import org.junit.Test;


import static org.junit.Assert.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;


public class SqlJetDiskHandlerTest {

	private static SQLJetDiskHandler diskHandler;

	@BeforeClass
	public static void init()  {
		diskHandler = new SQLJetDiskHandler();
	}


	@Test
	public void fetchBeforeWaitingTime()  {
		diskHandler.emptyDb();
		DbBulkRequest dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
		diskHandler.persistToDisk(dbBulkRequest);
		assertEquals(false, diskHandler.hasFailedBulks());
	}

	@Test
	public void fetchAfterWaitingTime()  {
		diskHandler.emptyDb();
		DbBulkRequest dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
		diskHandler.persistToDisk(dbBulkRequest);

		updateInsertTimeforTest(dbBulkRequest);

		assertEquals(true, diskHandler.hasFailedBulks());
	}


	@Test
	public void fetchFailedBulks()  {

		diskHandler.emptyDb();
		DbBulkRequest dbBulkRequest1 = MockBulkRequest.createMockDbBulkRequest();

		diskHandler.persistToDisk(dbBulkRequest1);
		updateInsertTimeforTest(dbBulkRequest1);
		List<DbBulkRequest> fetchedRequests = diskHandler.fetchFailedBulks(false);
		assertEquals(1, fetchedRequests.size());

		DbBulkRequest dbBulkRequest1FromDisk = fetchedRequests.get(0);
		DocWriteRequest<?> updateRequestFromDisk = dbBulkRequest1FromDisk.getRequest().requests().get(0);
		DocWriteRequest<?> updateRequest = dbBulkRequest1.getRequest().requests().get(0);

		assertEquals(dbBulkRequest1, dbBulkRequest1FromDisk);
		assertEquals(updateRequest.toString(), updateRequestFromDisk.toString());

		DbBulkRequest dbBulkRequest2 = MockBulkRequest.createMockDbBulkRequest();
		diskHandler.persistToDisk(dbBulkRequest2);
		updateInsertTimeforTest(dbBulkRequest2);
		assertEquals(2, diskHandler.failedBulksAmount());
	}

	@Test
	public void fetchTimesCounter()  {

		diskHandler.emptyDb();

		DbBulkRequest dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
		diskHandler.persistToDisk(dbBulkRequest);
		updateInsertTimeforTest(dbBulkRequest);
		DbBulkRequest fetchedRequest = diskHandler.fetchFailedBulks(false).get(0);
		diskHandler.persistToDisk(fetchedRequest);
		updateInsertTimeforTest(fetchedRequest);
		fetchedRequest=diskHandler.fetchFailedBulks(false).get(0);
		assertEquals(2, fetchedRequest.getTimesFetched());
	}

	@Test
	public void deleteBulk()  {
		diskHandler.emptyDb();
		DbBulkRequest dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
		diskHandler.persistToDisk(dbBulkRequest);

		updateInsertTimeforTest(dbBulkRequest);

		List<DbBulkRequest> fetchedRequests = diskHandler.fetchFailedBulks(false);
		assertEquals(1, fetchedRequests.size());

		diskHandler.deleteBulk(dbBulkRequest);
		assertEquals(false, diskHandler.hasFailedBulks());

	}


	@Test
	public void updateBulk() {
		diskHandler.emptyDb();
		DbBulkRequest dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
		diskHandler.persistToDisk(dbBulkRequest);
		diskHandler.updateBulk(dbBulkRequest.getId(),dbBulkRequest);

		updateInsertTimeforTest(dbBulkRequest);

		List<DbBulkRequest> fetchedRequests = diskHandler.fetchFailedBulks();
		DbBulkRequest dbBulkRequestFromDisk = fetchedRequests.get(0);
		assertEquals(1, dbBulkRequestFromDisk.getTimesFetched());
	}

	@AfterClass
	public static void tearDown(){
		diskHandler.emptyDb();
	}


	private void updateInsertTimeforTest(DbBulkRequest dbBulkRequest) {
		dbBulkRequest.setInsertTime((long) 0);
		diskHandler.updateBulk(dbBulkRequest.getId(), dbBulkRequest);
	}



}
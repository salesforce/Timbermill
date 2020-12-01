package com.datorama.oss.timbermill.common.disk;

import com.datorama.oss.timbermill.ElasticsearchClient;
import com.datorama.oss.timbermill.common.exceptions.MaximumInsertTriesException;
import com.datorama.oss.timbermill.unit.*;
import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.SerializationUtils;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.tmatesoft.sqljet.core.SqlJetException;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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

	@Test
	public void testMultiThreadSafety() throws MaximumInsertTriesException, InterruptedException {

        int numOfThreads = 15;
        AtomicBoolean isHealthCheckFailed = new AtomicBoolean(false);
        AtomicBoolean keepExecuting = new AtomicBoolean(true);
        ExecutorService executorService = Executors.newFixedThreadPool(numOfThreads);


        // insert some bulks to disk
        for (int i = 0 ; i < 10 ; i++){
            DbBulkRequest dbBulkRequest = MockBulkRequest.createMockDbBulkRequest();
            sqlJetDiskHandler.persistBulkRequestToDisk(dbBulkRequest, flowId, bulkNum);
        }


		Runnable fetchAndPersistTask = () -> {
			while (keepExecuting.get()) {
				try {
					sqlJetDiskHandler.healthCheck();
				} catch (SqlJetException e) {
					isHealthCheckFailed.set(true);
					break;
				}
				fetchAndPersist();
			}
		};

		for (int i = 0; i < numOfThreads; i++) {
			executorService.execute(fetchAndPersistTask);
		}

        Thread.sleep(2000);

		// stop threads and wait
        keepExecuting.set(false);
        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

		assertFalse(isHealthCheckFailed.get());
	}

	@Test
	public void validateEventsSerialization() throws IOException, URISyntaxException {

		boolean deserializationSuccess = true;

		String taskId = "taskId";
		String name = "name";
		String parentId = "parentId";
		TaskStatus taskStatus = TaskStatus.SUCCESS;
		String env = "env";
		String primaryId = "primaryId";

		/* Checking if no */
		Path path = Paths.get(SQLJetDiskHandlerTest.class.getResource("/old_version_event").toURI());
		List<Event> oldVersionEvents = null;
		byte[] oldVersionEventBytes = Files.readAllBytes(path);
		try {
			oldVersionEvents = sqlJetDiskHandler.deserializeEvents(oldVersionEventBytes);
		} catch (SerializationException e) {
			deserializationSuccess = false;
		}
		assertTrue("",deserializationSuccess);

		SpotEvent spotEvent = (SpotEvent) oldVersionEvents.get(0);
		String message = "A field in Event class that was changed will break the connection with the db, field name: ";
		assertEquals(message + "taskId", taskId, spotEvent.getTaskId());
		assertEquals(message + "name", name, spotEvent.getName());
		assertEquals(message + "primaryId", primaryId, spotEvent.getPrimaryId());
		assertEquals(message + "parentId", parentId, spotEvent.getParentId());
		assertEquals(message + "env", env, spotEvent.getEnv());
		assertEquals(message + "status", taskStatus, spotEvent.getStatus());
		assertNotEquals(message + "context", spotEvent.getContext(), null);
		assertNotEquals(message + "metrics", spotEvent.getMetrics(), null);
		assertNotEquals(message + "strings", spotEvent.getStrings(), null);
		assertNotEquals(message + "text", spotEvent.getText(), null);
		assertNotEquals(message + "parentsPath", spotEvent.getParentsPath(), null);
	}

	@Test
	public void validateBulkRequestsSerialization() throws IOException, URISyntaxException {

		boolean deserializationSuccess = true;

		Path path = Paths.get(SQLJetDiskHandlerTest.class.getResource("/old_version_bulk_request").toURI());
		BulkRequest oldVersionBulk = null;
		byte[] oldVersionBulkBytes = Files.readAllBytes(path);
		try {
			oldVersionBulk = sqlJetDiskHandler.deserializeBulkRequest(oldVersionBulkBytes);
		} catch (SerializationException e) {
			deserializationSuccess = false;
		}
		assertTrue(deserializationSuccess);

		String message = "A field in BulkRequest class that was changed will break the connection with the db, field name: ";
		assertEquals(message + "taskId", 3, oldVersionBulk.numberOfActions());
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


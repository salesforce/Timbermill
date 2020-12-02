package com.datorama.oss.timbermill.common.disk;

import com.datorama.oss.timbermill.ElasticsearchClient;
import com.datorama.oss.timbermill.common.exceptions.MaximumInsertTriesException;
import com.datorama.oss.timbermill.unit.*;
import org.apache.commons.lang3.SerializationException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.tmatesoft.sqljet.core.SqlJetException;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
	private static String OLD_VERSION_EVENT = "/old_version_event";
	private static String OLD_VERSION_BULK_REQUEST = "/old_version_bulk_request";

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
	public void validateBulkRequestsChanges() throws Exception {

		// Checking if can deserialize previous version of BulkRequest
		boolean deserializationSuccess = true;

		Path path = Paths.get(SQLJetDiskHandlerTest.class.getResource(OLD_VERSION_BULK_REQUEST).toURI());
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

	@Test
	public void validateEventsDeserialization() throws Exception {
		// Checking if can deserialize previous version of Event
		boolean deserializationSuccess = true;

		Path path = Paths.get(SQLJetDiskHandlerTest.class.getResource(OLD_VERSION_EVENT).toURI());
		byte[] oldVersionEventBytes = Files.readAllBytes(path);
		try {
			sqlJetDiskHandler.deserializeEvents(oldVersionEventBytes);
		} catch (SerializationException e) {
			deserializationSuccess = false;
		}

		String message = "Failed to deserialize previous version of Event, which may break " +
				"the connection with the db.\nYou may changed a field's type.";
		assertTrue(message, deserializationSuccess);
	}

	@Test
	public void validateEventsFieldNames() {
		/* Changing fields' names of Event class will pass validateEventsDeserialization test,
		but events in the db will be deserialized as null (since the new fields don't exist there).
		Therefore need to alert if one of Event's fields has been renamed */
		String renameError = "A field's name in Event class that was changed will break the connection with the db, field name: ";
		List<Class> eventClassList = new ArrayList<>(Arrays.asList(SpotEvent.class, StartEvent.class, SuccessEvent.class, ErrorEvent.class, InfoEvent.class));
		for (Class eventSubClass : eventClassList){
			Set<String> oldFields = getOldFields(eventSubClass);
			Set<String> newFields = getFields(eventSubClass);
			for (String OldField : oldFields) {
				assertTrue(renameError + OldField + " ,Event class: " + eventSubClass.getName(), newFields.contains(OldField));
			}
		}
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

	private Set<String> getOldFields(Class<? extends Event> clazz) {
		HashSet<String> oldFields = new HashSet<>();
		oldFields.add("taskId");
		oldFields.add("name");
		oldFields.add("primaryId");
		oldFields.add("parentId");
		oldFields.add("env");
		oldFields.add("time");
		oldFields.add("dateToDelete");
		oldFields.add("context");
		oldFields.add("metrics");
		oldFields.add("strings");
		oldFields.add("text");
		oldFields.add("parentsPath");
		if (clazz == SpotEvent.class){
			oldFields.add("status");
		}
		return oldFields;
	}

	private Set<String> getFields(Class clazz) {
		Set<String> fields = new HashSet<>();
		while (clazz.getSuperclass() != null) {
			for (Field field : clazz.getDeclaredFields()){
				fields.add(field.getName());
			}
			clazz = clazz.getSuperclass();
		}
		return fields;
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


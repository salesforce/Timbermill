package com.datorama.oss.timbermill.common.persistence;

import org.apache.commons.lang3.SerializationException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class SQLJetPersistenceHandlerTest extends PersistenceHandlerTest {

	@BeforeClass
	public static void init()  {
		Map<String, Object> persistenceHandlerParams = new HashMap<>();
		persistenceHandlerParams.put(PersistenceHandler.MAX_FETCHED_BULKS_IN_ONE_TIME, 10);
		persistenceHandlerParams.put(PersistenceHandler.MAX_FETCHED_EVENTS_IN_ONE_TIME, 3);
		persistenceHandlerParams.put(PersistenceHandler.MAX_INSERT_TRIES, 3);
		persistenceHandlerParams.put(SQLJetPersistenceHandler.LOCATION_IN_DISK, "/tmp/SQLJetPersistenceHandler");
		PersistenceHandlerTest.init(persistenceHandlerParams, "sqlite");
	}

	@Test
	public void hasFailedBulks() throws InterruptedException, ExecutionException {
		super.hasFailedBulks();
	}

	@Test
	public void fetchFailedBulks() throws InterruptedException, ExecutionException {
		super.fetchFailedBulks();
	}

	@Test
	public void fetchOverflowedEvents() throws InterruptedException, ExecutionException {
		super.fetchOverflowedEvents();
	}

	@Test
	public void fetchesCounter() throws InterruptedException, ExecutionException {
		super.fetchesCounter();
	}

	@Test
	public void failedBulksAmount() throws InterruptedException, ExecutionException {
		super.failedBulksAmount();
	}

	@Test
	public void overflowedEventsListsAmount() throws InterruptedException, ExecutionException {
		super.overflowedEventsListsAmount();
	}

	@Test
	public void fetchMaximumBulksAmount() throws InterruptedException, ExecutionException {
		super.fetchMaximumBulksAmount();
	}

	@Test
	public void fetchMaximumEventsAmount() throws InterruptedException, ExecutionException {
		super.fetchMaximumEventsAmount();
	}

	@Test
	public void dropAndRecreateTable() throws InterruptedException, ExecutionException {
		super.dropAndRecreateTable();
	}

	@Test
	public void testMultiThreadSafety() throws InterruptedException, ExecutionException {
		super.testMultiThreadSafety();
	}

	@Test
	public void validateEventsDeserialization() throws Exception {
		// Checking if can deserialize previous version of Event
		boolean deserializationSuccess = true;

		Path path = Paths.get(SQLJetPersistenceHandlerTest.class.getResource("/old_version_event").toURI());

		byte[] oldVersionEventBytes = Files.readAllBytes(path);
		try {
			((SQLJetPersistenceHandler)persistenceHandler).deserializeEvents(oldVersionEventBytes);
		} catch (SerializationException e) {
			deserializationSuccess = false;
		}

		String message = "Failed to deserialize previous version of Event, which may break " +
				"the connection with the db.\nYou may changed a field's type.";
		assertTrue(message, deserializationSuccess);
	}

	@Test
	public void failToInsert() throws ExecutionException, InterruptedException {
		DbBulkRequest dbBulkRequest = Mock.createMockDbBulkRequest();
		dbBulkRequest.setRequest(null); // will cause insert to fail

		persistenceHandler.persistBulkRequest(dbBulkRequest, bulkNum).get();
		assertFalse(persistenceHandler.hasFailedBulks());
	}

	@Test
	public void validateBulkRequestsDeserialization() throws Exception {
		// Checking if can deserialize previous version of BulkRequest
		boolean deserializationSuccess = true;

		Path path = Paths.get(SQLJetPersistenceHandlerTest.class.getResource("/old_version_bulk_request").toURI());

		BulkRequest oldVersionBulk = null;
		byte[] oldVersionBulkBytes = Files.readAllBytes(path);
		try {
			oldVersionBulk = ((SQLJetPersistenceHandler)persistenceHandler).deserializeBulkRequest(oldVersionBulkBytes);
		} catch (SerializationException e) {
			deserializationSuccess = false;
		}

		assertTrue(deserializationSuccess);

		String message = "A field in BulkRequest class that was changed will break the connection with the db, field name: ";
		assertEquals(message + "requests", 3, oldVersionBulk.numberOfActions());
	}

}


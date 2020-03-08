package com.datorama.oss.timbermill.common;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.junit.Test;

import static com.datorama.oss.timbermill.common.Constants.DEFAULT_ELASTICSEARCH_URL;
import static java.lang.Thread.sleep;
import static org.junit.Assert.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.datorama.oss.timbermill.TimberLogger;

public class SqlJetDiskHandlerTest {

	private static SQLJetDiskHandler diskHandler;
	private static boolean test = false;


	@BeforeClass
	public static void init()  {
		diskHandler = new SQLJetDiskHandler();
	}



	@Test
	public void fetchBeforeWaitingTime()  {
		diskHandler.emptyDb();
		DbBulkRequest dbBulkRequest = createMockDbBulkRequest();
		diskHandler.persistToDisk(dbBulkRequest);
		List<DbBulkRequest> fetchedRequests = diskHandler.fetchFailedBulks();
		assertEquals(0, fetchedRequests.size());
	}

	@Test
	public void fetchAfterWaitingTime()  {
		diskHandler.emptyDb();
		DbBulkRequest dbBulkRequest = createMockDbBulkRequest();
		diskHandler.persistToDisk(dbBulkRequest);

		updateInsertTimeforTest(dbBulkRequest);

		List<DbBulkRequest> fetchedRequests = diskHandler.fetchFailedBulks(false);
		assertEquals(1, fetchedRequests.size());
	}


	@Test
	public void fetchFailedBulks()  {

		diskHandler.emptyDb();
		DbBulkRequest dbBulkRequest1 = createMockDbBulkRequest();

		diskHandler.persistToDisk(dbBulkRequest1);
		updateInsertTimeforTest(dbBulkRequest1);
		List<DbBulkRequest> fetchedRequests = diskHandler.fetchFailedBulks(false);
		assertEquals(1, fetchedRequests.size());

		DbBulkRequest dbBulkRequest1FromDisk = fetchedRequests.get(0);
		DocWriteRequest<?> updateRequestFromDisk = dbBulkRequest1FromDisk.getRequest().requests().get(0);
		DocWriteRequest<?> updateRequest = dbBulkRequest1.getRequest().requests().get(0);

		assertEquals(dbBulkRequest1, dbBulkRequest1FromDisk);
		assertEquals(updateRequest.toString(), updateRequestFromDisk.toString());

		DbBulkRequest dbBulkRequest2 = createMockDbBulkRequest();
		diskHandler.persistToDisk(dbBulkRequest2);
		updateInsertTimeforTest(dbBulkRequest2);
		fetchedRequests = diskHandler.fetchFailedBulks(false);
		assertEquals(2, fetchedRequests.size());
	}


	@Test
	public void deleteBulk() {
		diskHandler.emptyDb();
		DbBulkRequest dbBulkRequest = createMockDbBulkRequest();
		diskHandler.persistToDisk(dbBulkRequest);

		updateInsertTimeforTest(dbBulkRequest);

		List<DbBulkRequest> fetchedRequests = diskHandler.fetchFailedBulks(false);
		assertEquals(1, fetchedRequests.size());

		diskHandler.deleteBulk(dbBulkRequest);
		fetchedRequests = diskHandler.fetchFailedBulks();
		assertEquals(0, fetchedRequests.size());

	}


	@Test
	public void updateBulk() {
		diskHandler.emptyDb();
		DbBulkRequest dbBulkRequest = createMockDbBulkRequest();
		diskHandler.persistToDisk(dbBulkRequest);
		diskHandler.updateBulk(dbBulkRequest.getId(),dbBulkRequest);

		updateInsertTimeforTest(dbBulkRequest);

		List<DbBulkRequest> fetchedRequests = diskHandler.fetchFailedBulks();
		DbBulkRequest dbBulkRequestFromDisk = fetchedRequests.get(0);
		assertEquals(1, dbBulkRequestFromDisk.getTimesFetched());
	}

	@AfterClass
	public static void tearDown(){
	}


	private static UpdateRequest createMockRequest() {
		String taskId = UUID.randomUUID().toString();
		String index = "timbermill-test";
		UpdateRequest updateRequest = new UpdateRequest(index, Constants.TYPE, taskId);
		Script script = new Script(ScriptType.STORED, null, Constants.TIMBERMILL_SCRIPT, new HashMap<>());
		updateRequest.script(script);
		return updateRequest;
	}

	private static DbBulkRequest createMockDbBulkRequest() {
		UpdateRequest updateRequest1 = createMockRequest();
		UpdateRequest updateRequest2 = createMockRequest();
		BulkRequest request = new BulkRequest();
		request.add(updateRequest1);
		request.add(updateRequest2);
		DbBulkRequest dbBulkRequest = new DbBulkRequest(request);
		return dbBulkRequest;
	}

	private void updateInsertTimeforTest(DbBulkRequest dbBulkRequest) {
		dbBulkRequest.setInsertTime((long) 0);
		diskHandler.updateBulk(dbBulkRequest.getId(), dbBulkRequest);
	}

}
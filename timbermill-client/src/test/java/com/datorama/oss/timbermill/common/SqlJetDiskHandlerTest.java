package com.datorama.oss.timbermill.common;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.junit.Test;

import static com.datorama.oss.timbermill.common.Constants.DEFAULT_ELASTICSEARCH_URL;
import static org.junit.Assert.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.datorama.oss.timbermill.TimberLogger;

public class SqlJetDiskHandlerTest {

	private static SQLJetDiskHandler diskHandler;

	@BeforeClass
	public static void init()  {
		diskHandler = new SQLJetDiskHandler();
	}


	@Test
	public void persistAndFetch()  {
		String taskId = "ctr___6ab93686_1d34_4a77_ad24_2b4403c05857";
		String index = "timbermill-test";
		UpdateRequest updateRequest = new UpdateRequest(index, Constants.TYPE, taskId);
		Script script = new Script(ScriptType.STORED, null, Constants.TIMBERMILL_SCRIPT, new HashMap<>());
		updateRequest.script(script);

		BulkRequest request = new BulkRequest();
		request.add(updateRequest);
		DbBulkRequest dbBulkRequest = new DbBulkRequest(request);

		diskHandler.persistToDisk(dbBulkRequest);
		List<DbBulkRequest> fetchedRequests = diskHandler.fetchFailedBulks();
		DbBulkRequest dbBulkRequestFromDisk = fetchedRequests.get(fetchedRequests.size() - 1);
		DocWriteRequest<?> updateRequestFromDisk = dbBulkRequestFromDisk.getRequest().requests().get(0);

		assertEquals(dbBulkRequest, dbBulkRequestFromDisk);
		assertEquals(updateRequest.toString(), updateRequestFromDisk.toString());
	}


	@Test
	public void fetchFailedBulks(){

	}

	@Test
	public void persistToDisk() {

	}

	@AfterClass
	public static void tearDown(){
		diskHandler.dropTable();
	}


}
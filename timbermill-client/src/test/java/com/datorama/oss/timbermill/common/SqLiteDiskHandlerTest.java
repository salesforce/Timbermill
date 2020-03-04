package com.datorama.oss.timbermill.common;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.junit.Test;

import static org.junit.Assert.*;

public class SqLiteDiskHandlerTest {

	@Test
	public void persistAndFetch() throws SQLException {
		String taskId = "ctr___6ab93686_1d34_4a77_ad24_2b4403c05857";
		String index = "timbermill-test";
		UpdateRequest updateRequest = new UpdateRequest(index, Constants.TYPE, taskId);
		Script script = new Script(ScriptType.STORED, null, Constants.TIMBERMILL_SCRIPT, new HashMap<>());
		updateRequest.script(script);

		BulkRequest request = new BulkRequest();
		request.add(updateRequest);
		TimbermillBulkRequest timbermillBulkRequest = new TimbermillBulkRequest(request);

		DiskHandler diskHandler = new SqLiteDiskHandler();
		diskHandler.persistToDisk(timbermillBulkRequest);
		List<TimbermillBulkRequest> fetchedRequests = diskHandler.fetchFailedBulks();
		TimbermillBulkRequest timbermillBulkRequestFromDisk = fetchedRequests.get(fetchedRequests.size() - 1);
		DocWriteRequest<?> updateRequestFromDisk = timbermillBulkRequestFromDisk.getRequest().requests().get(0);

		assertEquals(timbermillBulkRequest, timbermillBulkRequestFromDisk);
		assertEquals(updateRequest.toString(), updateRequestFromDisk.toString());


	}

	@Test
	public void fetchFailedBulks(){

	}

	@Test
	public void persistToDisk() {
	}
}
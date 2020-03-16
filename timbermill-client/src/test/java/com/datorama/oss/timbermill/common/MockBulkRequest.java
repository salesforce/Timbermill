package com.datorama.oss.timbermill.common;

import java.util.HashMap;
import java.util.UUID;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

public class MockBulkRequest {
	public static UpdateRequest createMockRequest() {
		String taskId = UUID.randomUUID().toString();
		String index = "timbermill-test";
		UpdateRequest updateRequest = new UpdateRequest(index, Constants.TYPE, taskId);
		Script script = new Script(ScriptType.STORED, null, Constants.TIMBERMILL_SCRIPT, new HashMap<>());
		updateRequest.script(script);
		return updateRequest;
	}

	public static DbBulkRequest createMockDbBulkRequest(int amountOfRequestsInBulk) {
		BulkRequest bulkRequest = new BulkRequest();
		for (int i = 0 ; i < amountOfRequestsInBulk ; i++){
			bulkRequest.add(createMockRequest());
		}
		DbBulkRequest dbBulkRequest = new DbBulkRequest(bulkRequest);
		return dbBulkRequest;
	}

	public static DbBulkRequest createMockDbBulkRequest() {
		return createMockDbBulkRequest(1);
	}
}

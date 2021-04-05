package com.datorama.timbermill.server.service;

import com.datorama.oss.timbermill.ElasticsearchClient;
import com.datorama.oss.timbermill.common.persistence.DbBulkRequest;
import com.datorama.oss.timbermill.common.persistence.PersistenceHandler;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.UUID;

@RestController
public class AdminController {

	private static final Logger LOG = LoggerFactory.getLogger(AdminController.class);

	@Autowired
	private TimbermillService timbermillService;


	@RequestMapping(method = RequestMethod.POST, value = "/persistence/bulk_requests/amount")
	public long persistenceBulkRequestsAmount() {
		LOG.info("Start bulks amount");
		long amount;
		PersistenceHandler persistenceHandler = timbermillService.getPersistenceHandler();
		if (persistenceHandler != null){
			amount = persistenceHandler.failedBulksAmount();
		} else {
			amount = -1;
		}
		LOG.info("End bulks amount");
		return amount;
	}

	@RequestMapping(method = RequestMethod.POST, value = "/persistence/overflowed_events/amount")
	public long persistenceOverflowedEventsListsAmount() {
		LOG.info("Start events amount");
		long amount;
		PersistenceHandler persistenceHandler = timbermillService.getPersistenceHandler();
		if (persistenceHandler != null){
			amount = persistenceHandler.overFlowedEventsListsAmount();
		} else {
			amount = -1;
		}
		LOG.info("End events amount");
		return amount;
	}

	@RequestMapping(method = RequestMethod.POST, value = "/persistence/reset")
	public void persistenceReset() {
		LOG.info("Start reset");
		PersistenceHandler persistenceHandler = timbermillService.getPersistenceHandler();
		if (persistenceHandler != null) {
			persistenceHandler.reset();
		}
		LOG.info("End reset");
	}

	@RequestMapping(method = RequestMethod.POST, value = "/persistence/persist")
	public void persistencePersist() {
		LOG.info("Test - Start persist mock requests");
		PersistenceHandler persistenceHandler = timbermillService.getPersistenceHandler();
		if (persistenceHandler != null) {
			for (int i = 0; i < 15; i++) {
				persistenceHandler.persistBulkRequest(createMockDbBulkRequest(), i);
			}
			LOG.info("Test - Finished persist mock requests");
		} else {
			LOG.info("Test - no persistence");
		}
	}

	private UpdateRequest createMockRequest() {
		String taskId = UUID.randomUUID().toString();
		String index = "timbermill-test";
		UpdateRequest updateRequest = new UpdateRequest(index, ElasticsearchClient.TYPE, taskId);
		Script script = new Script(ScriptType.STORED, null, ElasticsearchClient.TIMBERMILL_SCRIPT, new HashMap<>());
		updateRequest.script(script);
		return updateRequest;
	}
	private DbBulkRequest createMockDbBulkRequest() {
		BulkRequest bulkRequest = new BulkRequest();
		for (int i = 0 ; i < 3 ; i++){
			bulkRequest.add(createMockRequest());
		}
		return new DbBulkRequest(bulkRequest);
	}
}

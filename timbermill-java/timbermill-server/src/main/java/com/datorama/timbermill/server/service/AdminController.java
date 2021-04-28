package com.datorama.timbermill.server.service;

import com.datorama.oss.timbermill.common.persistence.DbBulkRequest;
import com.datorama.oss.timbermill.common.persistence.PersistenceHandler;
import org.elasticsearch.action.bulk.BulkRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AdminController {

	private static final Logger LOG = LoggerFactory.getLogger(AdminController.class);

	@Autowired
	private TimbermillService timbermillService;


	@RequestMapping(method = RequestMethod.POST, value = "/persistence/requests/amount")
	public long persistenceBulkRequestsAmount() {
		long amount;
		PersistenceHandler persistenceHandler = timbermillService.getPersistenceHandler();
		if (persistenceHandler != null){
			amount = persistenceHandler.failedBulksAmount();
		} else {
			amount = -1;
		}
		return amount;
	}

	@RequestMapping(method = RequestMethod.POST, value = "/persistence/events/amount")
	public long persistenceOverflowedEventsListsAmount() {
		long amount;
		PersistenceHandler persistenceHandler = timbermillService.getPersistenceHandler();
		if (persistenceHandler != null){
			amount = persistenceHandler.overFlowedEventsListsAmount();
		} else {
			amount = -1;
		}
		return amount;
	}

	@RequestMapping(method = RequestMethod.POST, value = "/persistence/reset")
	public void persistenceReset() {
		PersistenceHandler persistenceHandler = timbermillService.getPersistenceHandler();
		if (persistenceHandler != null) {
			persistenceHandler.reset();
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/persistence/persist")
	public void persistencePersistMocks() {
		LOG.info("Test - start persist mock requests");
		PersistenceHandler persistenceHandler = timbermillService.getPersistenceHandler();
		if (persistenceHandler != null) {
			for (int i = 0; i < 15; i++) {
				DbBulkRequest dbBulkRequest = new DbBulkRequest(new BulkRequest());
				persistenceHandler.persistBulkRequest(dbBulkRequest, i);
			}
			LOG.info("Test - finished persist mock requests");
		} else {
			LOG.info("Test - no persistence");
		}
	}
}

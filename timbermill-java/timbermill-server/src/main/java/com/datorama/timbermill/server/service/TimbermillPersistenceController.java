package com.datorama.timbermill.server.service;

import com.datorama.oss.timbermill.common.persistence.PersistenceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TimbermillPersistenceController {

	private static final Logger LOG = LoggerFactory.getLogger(TimbermillPersistenceController.class);

	@Autowired
	private TimbermillService timbermillService;


	@RequestMapping(method = RequestMethod.POST, value = "/persistence/bulk_requests/amount")
	public long persistenceBulkRequestsAmount() {
		PersistenceHandler persistenceHandler = timbermillService.getPersistenceHandler();
		if (persistenceHandler != null){
			return persistenceHandler.failedBulksAmount();
		} else {
			return -1;
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/persistence/overflowed_events/amount")
	public long persistenceOverflowedEventsListsAmount() {
		PersistenceHandler persistenceHandler = timbermillService.getPersistenceHandler();
		if (persistenceHandler != null){
			return persistenceHandler.overFlowedEventsListsAmount();
		} else {
			return -1;
		}
	}

	@RequestMapping(method = RequestMethod.POST, value = "/persistence/reset")
	public void persistenceReset() {
		PersistenceHandler persistenceHandler = timbermillService.getPersistenceHandler();
		if (persistenceHandler != null) {
			persistenceHandler.reset();
		}
	}

}

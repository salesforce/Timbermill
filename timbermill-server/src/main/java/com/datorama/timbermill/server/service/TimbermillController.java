package com.datorama.timbermill.server.service;

import java.util.Collection;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.datorama.oss.timbermill.unit.Event;
import com.datorama.oss.timbermill.unit.EventsWrapper;

@RestController
public class TimbermillController {

	@Autowired
	private TimbermillService timbermillService;

	@RequestMapping(method = RequestMethod.POST, value = "/events")
	public String ingestEvents(@RequestBody @Valid EventsWrapper eventsWrapper) {
		Collection<Event> events = eventsWrapper.getEvents();
		timbermillService.handleEvent(events);
		return "Event handled";
	}
}

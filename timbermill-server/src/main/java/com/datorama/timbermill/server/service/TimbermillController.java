package com.datorama.timbermill.server.service;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.util.ContentCachingRequestWrapper;

import com.datorama.oss.timbermill.unit.Event;
import com.datorama.oss.timbermill.unit.EventsWrapper;

import static com.datorama.oss.timbermill.common.Constants.GSON;

@RestController
public class TimbermillController {

	private static final Logger LOG = LoggerFactory.getLogger(TimbermillController.class);

	@Autowired
	private TimbermillService timbermillService;

	@RequestMapping(method = RequestMethod.POST, value = "/events")
	public String ingestEvents(@RequestBody @Valid EventsWrapper eventsWrapper) {
		Collection<Event> events = eventsWrapper.getEvents();
		timbermillService.handleEvent(events);

		Map<String, List<Event>> collect = events.stream().filter(e -> e.isStartEvent()).collect(Collectors.groupingBy(e -> e.getTaskId()));
		for (Map.Entry<String, List<Event>> stringListEntry : collect.entrySet()) {
			if (stringListEntry.getValue().size() > 1){
				LOG.warn("More than 1 start events found for id {}", stringListEntry.getKey());
				LOG.warn(GSON.toJson(stringListEntry.getValue()));
			}
		}
		return "Event handled";
	}

	@ExceptionHandler(Exception.class)
	public void handleEmployeeNotFoundException(HttpServletRequest request, Exception ex) throws IOException {
		ContentCachingRequestWrapper wrapper = (ContentCachingRequestWrapper) request;
		String body = IOUtils.toString(wrapper.getContentAsByteArray(), wrapper.getCharacterEncoding());
		LOG.error("Error parsing request. Body:\n " + body, ex);
	}
}

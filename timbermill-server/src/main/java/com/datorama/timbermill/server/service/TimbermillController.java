package com.datorama.timbermill.server.service;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.util.ContentCachingRequestWrapper;

import com.datorama.oss.timbermill.unit.Event;
import com.datorama.oss.timbermill.unit.EventsWrapper;

@RestController
public class TimbermillController {

	private static final Logger LOG = LoggerFactory.getLogger(TimbermillController.class);

	private static ExecutorService executorService;

	@Autowired
	private TimbermillService timbermillService;

	public TimbermillController(@Value("${RECEIVING_THREAD:10}") int numOfThreads) {
		executorService = Executors.newFixedThreadPool(numOfThreads);
	}

	@RequestMapping(method = RequestMethod.POST, value = "/events")
	public String ingestEvents(@RequestBody @Valid EventsWrapper eventsWrapper) {
		executorService.submit(() -> {
			Collection<Event> events = eventsWrapper.getEvents();
			timbermillService.handleEvent(events);
		});
		return "Event received";
	}

	@ExceptionHandler(HttpMessageNotReadableException.class)
	@ResponseBody
	public ResponseEntity<?> handleEmployeeNotFoundException(HttpServletRequest request, Exception ex) throws IOException {
		ContentCachingRequestWrapper wrapper = (ContentCachingRequestWrapper) request;
		String body = IOUtils.toString(wrapper.getContentAsByteArray(), wrapper.getCharacterEncoding());
		LOG.error("Error parsing request. Body:\n " + body, ex);
		return new ResponseEntity<>("Error parsing request: " + body, HttpStatus.BAD_REQUEST);
	}
}

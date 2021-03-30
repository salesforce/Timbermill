package com.datorama.timbermill.server.service;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
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

import com.datorama.oss.timbermill.unit.Event;
import com.datorama.oss.timbermill.unit.EventsList;
import com.datorama.oss.timbermill.unit.EventsWrapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

@RestController
public class TimbermillController {

	private static final Logger LOG = LoggerFactory.getLogger(TimbermillController.class);

	private static ExecutorService executorService;
	private final Cache<String, String> idsCache;

	@Autowired
	private TimbermillService timbermillService;

	public TimbermillController(@Value("${RECEIVING_THREAD:10}") int numOfThreads) {
		CacheBuilder<String, String> cacheBuilder = CacheBuilder.newBuilder().weigher((key, value) -> key.length() + value.length());
		idsCache = cacheBuilder
				.maximumWeight(1000000) //1MB
				.expireAfterWrite(10, TimeUnit.SECONDS)
				.build();
		executorService = Executors.newFixedThreadPool(numOfThreads);
	}

	@RequestMapping(method = RequestMethod.POST, value = "/events")
	public String ingestEvents(@RequestBody @Valid EventsWrapper eventsWrapper) {
		executorService.submit(() -> {
			String eventsId = eventsWrapper.getId();
			if (eventsId != null){
				if (idsCache.getIfPresent(eventsId) != null){
					LOG.warn("Got duplicated EventsWrapper {}", eventsWrapper.getEvents());
					return;
				}
				else{
					idsCache.put(eventsId, eventsId);
				}
			}
			Collection<Event> events = eventsWrapper.getEvents();
			timbermillService.handleEvents(events);
		});
		return "Event received";
	}

	@RequestMapping(method = RequestMethod.POST, value = "/events/v2")
	public String ingestEventsNew(@RequestBody @Valid EventsList events) {
		executorService.submit(() -> timbermillService.handleEvents(events));
		return "Event received";
	}

	@ExceptionHandler(HttpMessageNotReadableException.class)
	@ResponseBody
	public ResponseEntity<?> handleHttpMessageNotReadableException(HttpServletRequest request, Exception ex) throws IOException {
		HttpServletRequestWrapper wrapper = (HttpServletRequestWrapper) request;
		String body = IOUtils.toString(wrapper.getInputStream(), wrapper.getCharacterEncoding());
		LOG.error("Error parsing request. Body:\n " + body, ex);
		return new ResponseEntity<>("Error parsing request: " + body, HttpStatus.BAD_REQUEST);
	}
}

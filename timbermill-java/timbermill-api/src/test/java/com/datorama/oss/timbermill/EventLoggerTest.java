package com.datorama.oss.timbermill;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datorama.oss.timbermill.pipe.MockPipe;
import com.datorama.oss.timbermill.unit.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import static com.datorama.oss.timbermill.common.Constants.*;
import static org.junit.Assert.*;

public class EventLoggerTest {

	private static final String QUERY = "Query";
	private static final String SQL = "SQL";
	private static final String FAIL_MESSAGE = "fail";
	private static final String TEST = "test";
	private static final String PARAM = "Param";
	private static final String BOOTSTRAP = "bootstrap";
	private static final LogParams EMPTY_LOG_PARAMS = LogParams.create();
	private static EventLogger el;
	private static MockPipe mockPipe;


	@BeforeClass
	public static void init(){
		mockPipe = new MockPipe();
		EventLogger.bootstrap(mockPipe, false, ImmutableMap.of(BOOTSTRAP, TEST), TEST);
		el = EventLogger.get();
	}

	@After
	public void tearDown() {
		el.clearStack();
		EventLogger.exit();
		mockPipe.close();
	}

	@Test
	public void testSimpleEventLogger() {
		el.startEvent(QUERY, EMPTY_LOG_PARAMS);
		el.successEvent();
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> mockPipe.getCollectedEvents().size() == 2);
		List<Event> events = mockPipe.getCollectedEvents();

		Event startEvent = events.get(0);
		Event endEvent = events.get(1);
		assertEquals(QUERY, startEvent.getName());
		assertTrue(startEvent instanceof StartEvent);
		assertTrue(endEvent instanceof SuccessEvent);
		assertNull(startEvent.getTaskId(), startEvent.getPrimaryId());
		assertNull(startEvent.getParentId());
		assertEquals(TEST, startEvent.getStrings().get(BOOTSTRAP));
	}

	@Test
	public void testWrapCallable() throws Exception {
		String parentTaskId = el.startEvent(QUERY, EMPTY_LOG_PARAMS);
		Callable<String> wrappedCallable = el.wrapCallable(() -> {
			return EventLogger.get().getCurrentTaskId();
		});

		String childTaskId = el.startEvent(QUERY, EMPTY_LOG_PARAMS);

		String callableVisibleTaskId = wrappedCallable.call();

		assertEquals(parentTaskId, callableVisibleTaskId);
	}

	@Test
	public void testWrapCallableOnEmptyStack() throws Exception {
		Callable<String> wrappedCallable = el.wrapCallable(() -> {
			String parentTaskId = TimberLogger.getCurrentTaskId();
			String selfTaskId = TimberLogger.start("some_nifty_task");
			TimberLogger.success();
			return parentTaskId;
		});

		String callableVisibleTaskId = wrappedCallable.call();

		assertNull(callableVisibleTaskId);
	}

	@Test
	public void testWrapCallableThreaded() throws Exception {
		String parentTaskId = el.startEvent(QUERY, EMPTY_LOG_PARAMS);
		Callable<String> wrappedCallable = el.wrapCallable(() -> {
			return EventLogger.get().getCurrentTaskId();
		});

		String childTaskId = el.startEvent(QUERY, EMPTY_LOG_PARAMS);

		CompletableFuture<String> future = new CompletableFuture<String>();

		new Thread() {
			@Override
			public void run() {
				try {
					future.complete(wrappedCallable.call());
				} catch (Exception ex) {
					future.completeExceptionally(ex);
				}
			}
		}.start();

		String futureVisibleTaskId = future.get();

		assertEquals(parentTaskId, futureVisibleTaskId);
	}

	@Test
	public void testWrapFunction() throws Exception {
		String parentTaskId = el.startEvent(QUERY, EMPTY_LOG_PARAMS);
		Function<Integer, String> wrappedFunction = el.<Integer, String>wrapFunction(ignored -> {
			return EventLogger.get().getCurrentTaskId();
		});

		String childTaskId = el.startEvent(QUERY, EMPTY_LOG_PARAMS);

		String callableVisibleTaskId = wrappedFunction.apply(42);

		assertEquals(parentTaskId, callableVisibleTaskId);
	}

	@Test
	public void testFailedEventLogger() {
		el.startEvent(QUERY, EMPTY_LOG_PARAMS);
		Exception exception = new Exception(FAIL_MESSAGE);
		el.endWithError(exception);

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> mockPipe.getCollectedEvents().size() == 2);
		List<Event> events = mockPipe.getCollectedEvents();

		Event endEvent = events.get(1);
		assertTrue(endEvent instanceof ErrorEvent);
		assertTrue(endEvent.getText().get(EXCEPTION).contains(exception.toString()));
	}

	@Test
	public void testSimpleHierarchyEventLogger() {
		el.startEvent(QUERY, EMPTY_LOG_PARAMS);
		el.startEvent(SQL, EMPTY_LOG_PARAMS);
		el.successEvent();
		el.successEvent();

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> mockPipe.getCollectedEvents().size() == 4);
		List<Event> events = mockPipe.getCollectedEvents();

		Event parentEventStart = events.get(0);
		Event childEventStart = events.get(1);
		Event childEventEnd = events.get(2);
		Event parentEventEnd = events.get(3);
		assertEquals(QUERY, parentEventStart.getName());
		assertEquals(SQL, childEventStart.getName());
		assertTrue(parentEventStart instanceof StartEvent);
		assertTrue(childEventStart instanceof StartEvent);
		assertTrue(parentEventEnd instanceof SuccessEvent);
		assertTrue(childEventEnd instanceof SuccessEvent);
		assertEquals(parentEventStart.getTaskId(), childEventStart.getParentId());
		assertEquals(TEST, parentEventStart.getStrings().get(BOOTSTRAP));
		assertEquals(TEST, childEventStart.getStrings().get(BOOTSTRAP));
	}

	@Test
	public void testDiagnosticEvent() {
		String startId = el.startEvent(QUERY, EMPTY_LOG_PARAMS);
		el.logParams(LogParams.create().text(PARAM, TEST));
		el.successEvent();
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> mockPipe.getCollectedEvents().size() == 3);
		List<Event> events = mockPipe.getCollectedEvents();

		Event diagnosticEvent = events.get(1);
		assertEquals(startId, diagnosticEvent.getTaskId());
		assertTrue(diagnosticEvent instanceof InfoEvent);
		assertEquals(TEST, diagnosticEvent.getText().get(PARAM));

	}

	@Test
	public void testSpotEvent(){
		String startId = el.startEvent(QUERY, EMPTY_LOG_PARAMS);
		el.spotEvent(null, "Testing", null, EMPTY_LOG_PARAMS, TaskStatus.SUCCESS, null);
		el.successEvent();
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> mockPipe.getCollectedEvents().size() == 3);
		List<Event> filteredEvents = mockPipe.getCollectedEvents();

		Event spotEvent = filteredEvents.get(1);
		assertTrue(spotEvent instanceof SpotEvent);
		assertEquals(startId, spotEvent.getParentId());
		assertNull(startId, spotEvent.getPrimaryId());
		assertEquals("Testing", spotEvent.getName());
	}

	@Test
	public void testEndSuccessWithoutStart(){
		el.successEvent();
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> mockPipe.getCollectedEvents().size() == 1);
		List<Event> events = mockPipe.getCollectedEvents();

		assertEquals(1, events.size());
		Event event = events.get(0);
		assertEquals(LOG_WITHOUT_CONTEXT, event.getName());
		assertEquals(TEST, event.getStrings().get(BOOTSTRAP));
		assertTrue(event instanceof SpotEvent);
	}

	@Test
	public void testEndErrorWithoutStart(){
		Exception exception = new Exception(FAIL_MESSAGE);
		el.endWithError(exception);
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> mockPipe.getCollectedEvents().size() == 1);
		List<Event> events = mockPipe.getCollectedEvents();

		assertEquals(1, events.size());
		Event event = events.get(0);
		assertEquals(LOG_WITHOUT_CONTEXT, event.getName());
		assertEquals(TEST, event.getStrings().get(BOOTSTRAP));
		assertTrue(event instanceof SpotEvent);
		assertTrue(event.getText().get(EXCEPTION).contains(exception.toString()));
	}

	@Test
	public void testTextWithoutStart(){
		String key = "key";
		String value = "value";
		el.logParams(LogParams.create().text(key, value));
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> mockPipe.getCollectedEvents().size() == 1);
		List<Event> events = mockPipe.getCollectedEvents();

		assertEquals(1, events.size());
		Event event = events.get(0);
		assertEquals(LOG_WITHOUT_CONTEXT, event.getName());
		assertEquals(value, event.getText().get(key));
		assertEquals(TEST, event.getStrings().get(BOOTSTRAP));
		assertTrue(event instanceof SpotEvent);
	}

	@Test
	public void testStringsWithoutStart(){
		String key = "key";
		String value = "value";
		el.logParams(LogParams.create().string(key, value));
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> mockPipe.getCollectedEvents().size() == 1);
		List<Event> events = mockPipe.getCollectedEvents();

		assertEquals(1, events.size());
		Event event = events.get(0);
		assertEquals(LOG_WITHOUT_CONTEXT, event.getName());
		assertEquals(value, event.getStrings().get(key));
		assertEquals(TEST, event.getStrings().get(BOOTSTRAP));
		assertTrue(event instanceof SpotEvent);
	}

	@Test
	public void testMetricsWithoutStart(){
		String key = "key";
		el.logParams(LogParams.create().metric(key, 1));

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> mockPipe.getCollectedEvents().size() == 1);
		List<Event> events = mockPipe.getCollectedEvents();

		assertEquals(1, events.size());
		Event event = events.get(0);
		assertEquals(LOG_WITHOUT_CONTEXT, event.getName());
		assertEquals(1, event.getMetrics().get(key));
		assertEquals(TEST, event.getStrings().get(BOOTSTRAP));
		assertTrue(event instanceof SpotEvent);
	}

	@Test
	public void testSimpleTwoEventsFromDifferentThreads() {
		String taskId = el.startEvent(QUERY, EMPTY_LOG_PARAMS);
		el.successEvent();
		try (TimberLogContext ignored = new TimberLogContext(taskId)){
			el.startEvent(QUERY + '2', EMPTY_LOG_PARAMS);
			el.successEvent();
		}
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> mockPipe.getCollectedEvents().size() == 4);
		List<Event> events = mockPipe.getCollectedEvents();

		Event startEvent = events.get(2);
		Event endEvent = events.get(3);
		assertEquals(QUERY + '2', startEvent.getName());
		assertTrue(startEvent instanceof StartEvent);
		assertTrue(endEvent instanceof SuccessEvent);
		assertEquals(taskId, startEvent.getParentId());
	}

	@Test
	public void testEstimateSize() throws JsonProcessingException {
		LogParams params = LogParams.create().context("ctx", "ctx").context("ctx1", "ctx1").metric("metric", 7452).metric("metric1", 3265456).string("string", "string").string("string1", "string1").text("text1", "text1").text("text", "text");
		el.startEvent(QUERY, params);
		el.successEvent();
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> mockPipe.getCollectedEvents().size() == 2);
		List<Event> events = mockPipe.getCollectedEvents();

		Event startEvent = events.get(0);
		Event successEvent = events.get(1);
		assertTrue(closeEnoughSize(startEvent));
		assertTrue(closeEnoughSize(successEvent));
	}

	private boolean closeEnoughSize(Event event) throws JsonProcessingException {
		int actualSize = new ObjectMapper().writeValueAsString(event).length();
		int estimatedSize = event.estimatedSize();

		return Math.abs(actualSize - estimatedSize) < (actualSize / 20); //5%
	}
}
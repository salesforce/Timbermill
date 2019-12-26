package com.datorama.timbermill;

import com.datorama.timbermill.common.Constants;
import com.datorama.timbermill.pipe.MockPipe;
import com.datorama.timbermill.unit.*;
import com.google.common.collect.ImmutableMap;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.datorama.timbermill.common.Constants.EXCEPTION;
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
		assertEquals(startEvent.getTaskId(), startEvent.getPrimaryId());
		assertNull(startEvent.getParentId());
		assertEquals(TEST, startEvent.getContext().get(BOOTSTRAP));
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
		assertTrue(endEvent.getTexts().get(EXCEPTION).contains(exception.toString()));
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
		assertEquals(TEST, parentEventStart.getContext().get(BOOTSTRAP));
		assertEquals(TEST, childEventStart.getContext().get(BOOTSTRAP));
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
		assertEquals(TEST, diagnosticEvent.getTexts().get(PARAM));

	}

	@Test
	public void testSpotEvent(){
		String startId = el.startEvent(QUERY, EMPTY_LOG_PARAMS);
		el.spotEvent("Testing", EMPTY_LOG_PARAMS, Task.TaskStatus.SUCCESS, null);
		el.successEvent();
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> mockPipe.getCollectedEvents().size() == 3);
		List<Event> filteredEvents = mockPipe.getCollectedEvents();

		Event spotEvent = filteredEvents.get(1);
		assertTrue(spotEvent instanceof SpotEvent);
		assertEquals(startId, spotEvent.getParentId());
		assertEquals(startId, spotEvent.getPrimaryId());
		assertEquals("Testing", spotEvent.getName());
	}

	@Test
	public void testEndSuccessWithoutStart(){
		el.successEvent();
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> mockPipe.getCollectedEvents().size() == 1);
		List<Event> events = mockPipe.getCollectedEvents();

		assertEquals(1, events.size());
		Event event = events.get(0);
		assertEquals(Constants.LOG_WITHOUT_CONTEXT, event.getName());
		assertEquals(TEST, event.getContext().get(BOOTSTRAP));
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
		assertEquals(Constants.LOG_WITHOUT_CONTEXT, event.getName());
		assertEquals(TEST, event.getContext().get(BOOTSTRAP));
		assertTrue(event instanceof SpotEvent);
		assertTrue(event.getTexts().get(EXCEPTION).contains(exception.toString()));
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
		assertEquals(Constants.LOG_WITHOUT_CONTEXT, event.getName());
		assertEquals(value, event.getTexts().get(key));
		assertEquals(TEST, event.getContext().get(BOOTSTRAP));
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
		assertEquals(Constants.LOG_WITHOUT_CONTEXT, event.getName());
		assertEquals(value, event.getStrings().get(key));
		assertEquals(TEST, event.getContext().get(BOOTSTRAP));
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
		assertEquals(Constants.LOG_WITHOUT_CONTEXT, event.getName());
		assertEquals(1, event.getMetrics().get(key));
		assertEquals(TEST, event.getContext().get(BOOTSTRAP));
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
}

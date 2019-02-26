package com.datorama.timbermill;

import com.datorama.timbermill.pipe.MockPipe;
import com.google.common.collect.ImmutableMap;
import com.jayway.awaitility.Awaitility;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class EventLoggerTest {

	private static final String QUERY = "Query";
	private static final String SQL = "SQL";
	private static final String FAIL_MESSAGE = "fail";
	private static final String TEST = "test";
	private static final String PARAM = "Param";
	private static final String BOOTSTRAP = "bootstrap";
	private static EventLogger el;
	private static MockPipe mockPipe;


	@BeforeClass
	public static void init(){
		mockPipe = new MockPipe();
		EventLogger.bootstrap(ImmutableMap.of(BOOTSTRAP, TEST), mockPipe, false);
		el = EventLogger.get();
	}

	@After
	public void tearDown(){
		mockPipe.cleanQueue();
	}

	@Test
	public void testSimpleEventLogger() {
		el.startEvent(QUERY);
		el.successEvent();

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> mockPipe.getCollectedEvents().size() == 2);
		List<Event> events = mockPipe.getCollectedEvents();


		Event startEvent = events.get(0);
		Event endEvent = events.get(1);
		assertEquals(QUERY, startEvent.getTaskType());
		assertEquals(EventType.START, startEvent.getEventType());
		assertEquals(EventType.END_SUCCESS, endEvent.getEventType());
		assertEquals(startEvent.getTaskId(), startEvent.getPrimaryTaskId());
		assertNull(startEvent.getParentTaskId());
		assertEquals(TEST, startEvent.getAttributes().get(BOOTSTRAP));
	}

	@Test
	public void testFailedEventLogger() {

		el.startEvent(QUERY);

		Exception exception = new Exception(FAIL_MESSAGE);
		el.endWithError(exception);

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> mockPipe.getCollectedEvents().size() == 2);
		List<Event> events = mockPipe.getCollectedEvents();


		Event endEvent = events.get(1);
		assertEquals(EventType.END_ERROR, endEvent.getEventType());
		assertTrue(endEvent.getData().get(EventLogger.EXCEPTION).contains(exception.toString()));
	}

	@Test
	public void testFailedWithAppErrorLogger() {

		el.startEvent(QUERY);
		Exception exception = new Exception(FAIL_MESSAGE);
		el.endWithAppError(exception);

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> mockPipe.getCollectedEvents().size() == 2);
		List<Event> events = mockPipe.getCollectedEvents();

		Event startEvent = events.get(0);
		Event endEvent = events.get(1);
		assertEquals(QUERY, startEvent.getTaskType());
		assertEquals(EventType.START, startEvent.getEventType());
		assertEquals(EventType.END_APP_ERROR, endEvent.getEventType());
		assertTrue(endEvent.getData().get(EventLogger.EXCEPTION).contains(exception.toString()));
	}

	@Test
	public void testSimpleHierarchyEventLogger() {

		el.startEvent(QUERY);
		el.startEvent(SQL);
		el.successEvent();
		el.successEvent();

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> mockPipe.getCollectedEvents().size() == 4);
		List<Event> events = mockPipe.getCollectedEvents();


		Event parentEventStart = events.get(0);
		Event childEventStart = events.get(1);
		Event childEventEnd = events.get(2);
		Event parentEventEnd = events.get(3);
		assertEquals(QUERY, parentEventStart.getTaskType());
		assertEquals(SQL, childEventStart.getTaskType());
		assertEquals(EventType.START, parentEventStart.getEventType());
		assertEquals(EventType.START, childEventStart.getEventType());
		assertEquals(EventType.END_SUCCESS, parentEventEnd.getEventType());
		assertEquals(EventType.END_SUCCESS, childEventEnd.getEventType());
		assertEquals(parentEventStart.getTaskId(), childEventStart.getParentTaskId());
		assertEquals(parentEventStart.getTaskId(), childEventEnd.getParentTaskId());
		assertEquals(parentEventStart.getTaskId(), childEventEnd.getPrimaryTaskId());
		assertEquals(TEST, parentEventStart.getAttributes().get(BOOTSTRAP));
		assertEquals(TEST, childEventStart.getAttributes().get(BOOTSTRAP));
	}

	@Test
	public void testDiagnosticEvent() {

		String startId = el.startEvent(QUERY);
		el.logData(ImmutableMap.of(PARAM, TEST));
		el.successEvent();


		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> mockPipe.getCollectedEvents().size() == 3);
		List<Event> events = mockPipe.getCollectedEvents();


		Event diagnosticEvent = events.get(1);
		assertEquals(startId, diagnosticEvent.getTaskId());
		assertEquals(EventType.INFO, diagnosticEvent.getEventType());
		assertEquals(TEST, diagnosticEvent.getData().get(PARAM));

	}

	@Test
	public void testSpotEvent(){

		String startId = el.startEvent(QUERY);
		el.spotEvent("Testing", null, null, null, null);
		el.successEvent();


		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> mockPipe.getCollectedEvents().size() == 3);
		List<Event> filteredEvents = mockPipe.getCollectedEvents();


		Event spotEvent = filteredEvents.get(1);
		assertEquals(EventType.SPOT, spotEvent.getEventType());
		assertEquals(startId, spotEvent.getParentTaskId());
		assertEquals(startId, spotEvent.getPrimaryTaskId());
		assertEquals("Testing", spotEvent.getTaskType());

	}

	@Test
	public void testEndSuccessWithoutStart(){
		el.successEvent();
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> mockPipe.getCollectedEvents().size() == 1);
		List<Event> events = mockPipe.getCollectedEvents();


		assertEquals(1, events.size());
		Event event = events.get(0);
		assertEquals(Constants.END_WITHOUT_START, event.getTaskType());
		assertEquals(TEST, event.getAttributes().get(BOOTSTRAP));
		assertEquals(EventType.SPOT, event.getEventType());
	}

	@Test
	public void testEndErrorWithoutStart(){
		Exception exception = new Exception(FAIL_MESSAGE);
		el.endWithError(exception);

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> mockPipe.getCollectedEvents().size() == 1);
		List<Event> events = mockPipe.getCollectedEvents();


		assertEquals(1, events.size());
		Event event = events.get(0);
		assertEquals(Constants.END_WITHOUT_START, event.getTaskType());
		assertEquals(TEST, event.getAttributes().get(BOOTSTRAP));
		assertEquals(EventType.SPOT, event.getEventType());
		assertTrue(event.getData().get(EventLogger.EXCEPTION).contains(exception.toString()));
	}

	@Test
	public void testEndAppErrorWithoutStart(){
		Exception exception = new Exception(FAIL_MESSAGE);
		el.endWithAppError(exception);
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> mockPipe.getCollectedEvents().size() == 1);
		List<Event> events = mockPipe.getCollectedEvents();

		assertEquals(1, events.size());
		Event event = events.get(0);
		assertEquals(Constants.END_WITHOUT_START, event.getTaskType());
		assertEquals(TEST, event.getAttributes().get(BOOTSTRAP));
		assertEquals(EventType.SPOT, event.getEventType());
		assertTrue(event.getData().get(EventLogger.EXCEPTION).contains(exception.toString()));
	}

	@Test
	public void testDataWithoutStart(){
		String key = "key";
		String value = "value";
		el.logData(ImmutableMap.of(key, value));

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> mockPipe.getCollectedEvents().size() == 1);
		List<Event> events = mockPipe.getCollectedEvents();


		assertEquals(1, events.size());
		Event event = events.get(0);
		assertEquals(Constants.LOG_WITHOUT_CONTEXT, event.getTaskType());
		assertEquals(value, event.getData().get(key));
		assertEquals(TEST, event.getAttributes().get(BOOTSTRAP));
		assertEquals(EventType.SPOT, event.getEventType());
	}

	@Test
	public void testAttributeWithoutStart(){
		String key = "key";
		String value = "value";
		el.logAttributes(ImmutableMap.of(key, value));

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> mockPipe.getCollectedEvents().size() == 1);
		List<Event> events = mockPipe.getCollectedEvents();


		assertEquals(1, events.size());
		Event event = events.get(0);
		assertEquals(Constants.LOG_WITHOUT_CONTEXT, event.getTaskType());
		assertEquals(value, event.getAttributes().get(key));
		assertEquals(TEST, event.getAttributes().get(BOOTSTRAP));
		assertEquals(EventType.SPOT, event.getEventType());
	}

	@Test
	public void testMetricsWithoutStart(){
		String key = "key";
		el.logMetrics(ImmutableMap.of(key, 1));

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> mockPipe.getCollectedEvents().size() == 1);
		List<Event> events = mockPipe.getCollectedEvents();


		assertEquals(1, events.size());
		Event event = events.get(0);
		assertEquals(Constants.LOG_WITHOUT_CONTEXT, event.getTaskType());
		assertEquals(1, event.getMetrics().get(key));
		assertEquals(TEST, event.getAttributes().get(BOOTSTRAP));
		assertEquals(EventType.SPOT, event.getEventType());
	}

	@Test
	public void testSimpleTwoEventsFromDifferentThreads() {
		String taskId = el.startEvent(QUERY);
		el.successEvent();

		el.startEvent(QUERY + '2', taskId);
		el.successEvent();
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> mockPipe.getCollectedEvents().size() == 4);
		List<Event> events = mockPipe.getCollectedEvents();


		Event startEvent = events.get(2);
		Event endEvent = events.get(3);
		assertEquals(QUERY + '2', startEvent.getTaskType());
		assertEquals(EventType.START, startEvent.getEventType());
		assertEquals(EventType.END_SUCCESS, endEvent.getEventType());
		assertEquals(taskId, startEvent.getParentTaskId());
	}

}

package com.datorama.timbermill.pipe;

import com.datorama.timbermill.Event;
import com.datorama.timbermill.EventType;
import com.jayway.awaitility.Awaitility;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class BufferingOutputPipeTest {

	private MockPipe mockPipe;
	private BufferingOutputPipe bufferingOutputPipe;

	@Before
	public void setUp(){
		mockPipe = new MockPipe();
		bufferingOutputPipe = new BufferingOutputPipe(mockPipe);

	}

	@Test
	public void testSimpleEventsInsertion() {
		bufferingOutputPipe.start();
		Event startEvent = new Event("Event", EventType.START, new DateTime());
		Event endEvent = new Event("Event", EventType.END_SUCCESS, new DateTime());
		bufferingOutputPipe.send(startEvent);
		bufferingOutputPipe.send(endEvent);
		Awaitility.await().atMost(10, TimeUnit.SECONDS).pollInterval(10, TimeUnit.MILLISECONDS).until(() -> mockPipe.getCollectedEvents().size() == 2);
	}

	@Test
	public void testOverCapacityInsertion() {

		for (int i = 1 ; i <= 1000 ; i ++) {
			Event event = new Event("Event" + i, EventType.START, new DateTime());
			bufferingOutputPipe.send(event);
		}
		bufferingOutputPipe.start();
		Awaitility.await().atMost(10, TimeUnit.SECONDS).pollInterval(10, TimeUnit.MILLISECONDS).until(() -> mockPipe.getCollectedEvents().size() == 1000);
		assertEquals("Event1", mockPipe.getCollectedEvents().get(0).getTaskId());

	}
}
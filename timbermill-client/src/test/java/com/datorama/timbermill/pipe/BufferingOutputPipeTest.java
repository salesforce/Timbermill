package com.datorama.timbermill.pipe;

import com.datorama.timbermill.unit.Event;
import com.datorama.timbermill.unit.LogParams;
import com.datorama.timbermill.unit.StartEvent;
import com.datorama.timbermill.unit.SuccessEvent;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class BufferingOutputPipeTest {

	private MockPipe mockPipe;
	private BufferingOutputPipe bufferingOutputPipe;

	@Before
	public void setUp(){
		mockPipe = new MockPipe(Collections.EMPTY_MAP);
		bufferingOutputPipe = new BufferingOutputPipe(mockPipe);

	}

	@Test
	public void testSimpleEventsInsertion() {
		bufferingOutputPipe.start();
		LogParams logParams = LogParams.create();
		Event startEvent = new StartEvent("Event", logParams, null, null);
		Event endEvent = new SuccessEvent("Event", logParams);
		bufferingOutputPipe.send(startEvent);
		bufferingOutputPipe.send(endEvent);
		Awaitility.await().atMost(10, TimeUnit.SECONDS).pollInterval(10, TimeUnit.MILLISECONDS).until(() -> mockPipe.getCollectedEvents().size() == 2);
	}

	@Test
	public void testOverCapacityInsertion() {
		LogParams logParams = LogParams.create();
		for (int i = 1 ; i <= 1000 ; i ++) {
			Event event = new StartEvent("Event" + i, logParams, null, null);
			bufferingOutputPipe.send(event);
		}
		bufferingOutputPipe.start();
		Awaitility.await().atMost(10, TimeUnit.SECONDS).pollInterval(10, TimeUnit.MILLISECONDS).until(() -> mockPipe.getCollectedEvents().size() == 1000);
		assertEquals("Event1", mockPipe.getCollectedEvents().get(0).getName());

	}
}
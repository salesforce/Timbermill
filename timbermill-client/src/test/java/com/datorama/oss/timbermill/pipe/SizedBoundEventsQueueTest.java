package com.datorama.oss.timbermill.pipe;

import java.util.List;

import org.junit.Test;

import com.datorama.oss.timbermill.unit.Event;
import com.datorama.oss.timbermill.unit.LogParams;
import com.datorama.oss.timbermill.unit.StartEvent;

import static org.junit.Assert.*;

public class SizedBoundEventsQueueTest {

	@Test
	public void queueTest() {
		SizedBoundEventsQueue events = new SizedBoundEventsQueue(100000, 5, 100000, 100000);
		Event e1 = new StartEvent("id1", "name1", LogParams.create(), null);
		Event e2 = new StartEvent("id2", "name2", LogParams.create(), null);
		Event e3 = new StartEvent("id3", "name3", LogParams.create(), null);
		events.offer(e1);
		events.offer(e2);
		events.offer(e3);
		Event e1ret = events.poll();
		Event e2ret = events.poll();
		Event e3ret = events.poll();
		assertEquals(e1, e1ret);
		assertEquals(e2, e2ret);
		assertEquals(e3, e3ret);
	}

	@Test
	public void getEventsOfSizeTest() {
		SizedBoundEventsQueue events = new SizedBoundEventsQueue(100000, 5, 100000, 100000);
		Event e1 = new StartEvent("id1", "name1", LogParams.create(), null);
		Event e2 = new StartEvent("id2", "name2", LogParams.create(), null);
		Event e3 = new StartEvent("id3", "name3", LogParams.create(), null);

		e1.cleanEvent(100000, 100000);
		e2.cleanEvent(100000, 100000);
		e3.cleanEvent(100000, 100000);

		events.offer(e1);
		events.offer(e2);
		events.offer(e3);

		List<Event> eventsOfSize = events.getEventsOfSize(e1.estimatedSize() + e2.estimatedSize() - 2);
		assertEquals(2, eventsOfSize.size());
		assertEquals(e1, eventsOfSize.get(0));
		assertEquals(e2, eventsOfSize.get(1));
		assertEquals(e3.estimatedSize(), events.size());
	}
}
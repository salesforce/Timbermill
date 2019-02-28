package com.datorama.timbermill.pipe;

import com.datorama.timbermill.unit.Event;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MockPipe implements EventOutputPipe{


    private Queue<Event> queue;
    private List<Event> collectedEvents;

    public MockPipe() {
        collectedEvents = Collections.synchronizedList(new ArrayList<>());
        queue = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void send(Event e) {
        queue.add(e);
        collectedEvents.add(e);
    }

    @Override public int getMaxQueueSize() {
        return 1000;
    }

    @Override
    public void close() {
        queue.clear();
        collectedEvents.clear();
    }

    public List<Event> getCollectedEvents() {
        return Collections.unmodifiableList(collectedEvents);
    }

}

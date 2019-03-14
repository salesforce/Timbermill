package com.datorama.timbermill.pipe;

import com.datorama.timbermill.unit.Event;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MockPipe implements EventOutputPipe{


    private List<Event> collectedEvents;

    public MockPipe() {
        collectedEvents = Collections.synchronizedList(new ArrayList<>());
    }

    @Override
    public void send(Event e) {
        collectedEvents.add(e);
    }

    @Override public int getMaxQueueSize() {
        return 1000;
    }

    @Override
    public void close() {
        collectedEvents.clear();
    }

    public List<Event> getCollectedEvents() {
        return Collections.unmodifiableList(collectedEvents);
    }

}

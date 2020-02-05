package com.datorama.timbermill.pipe;

import com.datorama.timbermill.unit.Event;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MockPipe implements EventOutputPipe{

    private final List<Event> collectedEvents = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void send(Event e) {
        collectedEvents.add(e);
    }

    public void close() {
        collectedEvents.clear();
    }

    @Override public int getCurrentBufferSize() {
        return collectedEvents.size();
    }

    public List<Event> getCollectedEvents() {
        return Collections.unmodifiableList(collectedEvents);
    }

}

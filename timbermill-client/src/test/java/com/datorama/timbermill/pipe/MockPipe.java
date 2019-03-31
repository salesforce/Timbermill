package com.datorama.timbermill.pipe;

import com.datorama.timbermill.unit.Event;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MockPipe implements EventOutputPipe{


    private List<Event> collectedEvents;
    private Map<String, String> staticParams;

    public MockPipe(Map<String, String> staticParams) {
        this.staticParams = staticParams;
        collectedEvents = Collections.synchronizedList(new ArrayList<>());
    }

    @Override
    public void send(Event e) {
        collectedEvents.add(e);
    }

    @Override
    public void close() {
        collectedEvents.clear();
    }

    @Override
    public Map<String, String> getStaticParams() {
        return staticParams;
    }

    public List<Event> getCollectedEvents() {
        return Collections.unmodifiableList(collectedEvents);
    }

}

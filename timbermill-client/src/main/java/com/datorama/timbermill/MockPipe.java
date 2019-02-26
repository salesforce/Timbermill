package com.datorama.timbermill;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MockPipe implements EventOutputPipe, EventInputPipe {


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
    public List<Event> read(int maxEvent) {
        int i = 1;
        List<Event> returnEvents = new ArrayList<>();
        while((i <= maxEvent) && !queue.isEmpty()) {
			Event event = queue.poll();
			if (event != null){
				returnEvents.add(event);
			}
			i++;
        }
        return returnEvents;
    }

    public void cleanQueue() {
        queue.clear();
        collectedEvents.clear();
    }

    public List<Event> getCollectedEvents() {
        return Collections.unmodifiableList(collectedEvents);
    }
}

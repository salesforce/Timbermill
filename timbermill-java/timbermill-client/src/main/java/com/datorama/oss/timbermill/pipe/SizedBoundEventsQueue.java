package com.datorama.oss.timbermill.pipe;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.datorama.oss.timbermill.unit.Event;

class SizedBoundEventsQueue extends LinkedBlockingQueue<Event> {
	private AtomicInteger size = new AtomicInteger(0);

	private int maxSize;
	private long maxSecondsBeforeBatchTimeout;

	SizedBoundEventsQueue(int maxSize, long maxSecondsBeforeBatchTimeout) {
		this.maxSize = maxSize;
		this.maxSecondsBeforeBatchTimeout = maxSecondsBeforeBatchTimeout;
	}

	@Override
	public boolean offer(Event e) {
		int eventSize = e.estimatedSize();
		synchronized (this) {
			if (size.get() + eventSize <= maxSize) {
				size.addAndGet(eventSize);
				return super.offer(e);
			} else {
				return false;
			}
		}
	}

	@Override
	public Event poll() {
		synchronized (this) {
			Event event = super.poll();
			if (event != null) {
				size.addAndGet(-event.estimatedSize());
			}
			return event;
		}
	}

	List<Event> getEventsOfSize(int maxEventsBatchSize) {
		List<Event> eventsToSend = new ArrayList<>();
		try {
			int currentBatchSize = addEventFromBufferToList(eventsToSend);
			long startBatchTime = System.currentTimeMillis();
			while(currentBatchSize <= maxEventsBatchSize && !isExceededMaxTimeToWait(startBatchTime)) {
				currentBatchSize  += addEventFromBufferToList(eventsToSend);
			}
		} catch (InterruptedException e) {
			// If blocking queue poll timed out send current batch
		}
		return eventsToSend;
	}

	private int addEventFromBufferToList(List<Event> eventsToSend) throws InterruptedException {
		Event event = this.poll();
		if (event == null){
			Thread.sleep(100);
			return 0;
		}
		event.replaceAllFieldsWithDots();
		event.trimAllStrings();
		eventsToSend.add(event);
		return event.estimatedSize();
	}

	private boolean isExceededMaxTimeToWait(long startBatchTime) {
		return System.currentTimeMillis() - startBatchTime > maxSecondsBeforeBatchTimeout * 1000;
	}


	@Override
	public int size() {
		return size.get();
	}
}

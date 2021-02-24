package com.datorama.oss.timbermill.pipe;

import java.util.concurrent.atomic.AtomicLong;

import com.datorama.oss.timbermill.unit.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatisticsCollectorOutputPipe implements EventOutputPipe {

	private static final Logger LOG = LoggerFactory.getLogger(StatisticsCollectorOutputPipe.class);

	private EventOutputPipe delegate = new BlackHolePipe();
	private final AtomicLong eventsAmount = new AtomicLong(0);
	private final AtomicLong totalSubmitDuration = new AtomicLong(0);
	private final AtomicLong maxSubmitDuration = new AtomicLong(0);

	public StatisticsCollectorOutputPipe(EventOutputPipe delegate) {
		if (delegate == null){
			LOG.error("EventOutputPipe sent to Timbermill was null, Timbermill will be disabled.");
		}
		else {
			this.delegate = delegate;
		}
	}

	@Override public void send(Event e) {
		long start = System.currentTimeMillis();
		delegate.send(e);
		long end = System.currentTimeMillis();
		updateCounters(end-start);
	}

	private void updateCounters(long duration) {
		totalSubmitDuration.addAndGet(duration);
		long curMax = maxSubmitDuration.get();
		if (duration > curMax) {
			// This update might "fail" if cur max has changed in the meantime.
			// Since this is not mission critical, we will live with this situation
			maxSubmitDuration.compareAndSet(curMax, duration);
		}
		eventsAmount.incrementAndGet();
	}

	@Override public int getCurrentBufferSize() {
		return delegate.getCurrentBufferSize();
	}

	@Override public void close() {
		delegate.close();
	}

	public void initCounters() {
		eventsAmount.set(0);
		totalSubmitDuration.set(0);
		maxSubmitDuration.set(0);
	}

	public long getEventsAmount() {
		return eventsAmount.get();
	}

	public long getMaxSubmitDuration() {
		return maxSubmitDuration.get();
	}

	private long getTotalSubmitDuration() {
		return totalSubmitDuration.get();
	}

	public double getAvgSubmitDuration() {
		if (getEventsAmount() > 0) {
			return (double) getTotalSubmitDuration() / getEventsAmount();
		} else {
			return 0;
		}
	}

	@Override public String toString() {
		return String.format("eventsAmount:%d, avgSubmitDuration: %f, maxSubmitDuration:%d",
				getEventsAmount(), getAvgSubmitDuration(), getMaxSubmitDuration());
	}
}

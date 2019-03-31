package com.datorama.timbermill.pipe;

import com.datorama.timbermill.unit.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class BufferingOutputPipe implements EventOutputPipe {

	private final EventOutputPipe eventOutputPipe;
	private final BlockingQueue<Event> buffer;

	private static final int WARNING_BUFFER_SIZE = 100000;
	private static final int MAX_BUFFER_SIZE = 2000000;
	private static final int SLEEP_ON_ERROR_MILLIS = 10000;
	private static final Logger LOG = LoggerFactory.getLogger(BufferingOutputPipe.class);

	public BufferingOutputPipe(EventOutputPipe eventOutputPipe) {
		this.eventOutputPipe = eventOutputPipe;
		buffer = new ArrayBlockingQueue<>(MAX_BUFFER_SIZE + 1000);
	}

	public void start() {
		Thread bufferConsumer = new Thread(() -> {
			try {
				while (true) {
					Event e = buffer.take();
					try {
						eventOutputPipe.send(e);
					} catch (Throwable t) {
						LOG.warn("Failed sending event down the pipe, going to rest for a few... \n Exception: ", t);
						Thread.sleep(SLEEP_ON_ERROR_MILLIS);
					}
				}
			} catch (InterruptedException ignored) {
				// if interrupted --> exit cleanly
			}
		});
		bufferConsumer.setName("Timbermill-BufferingOutputPipeSubmitterThread");
		bufferConsumer.setDaemon(true);
		bufferConsumer.start();
	}

	@Override
	public void send(Event e) {
		if (buffer.size() > WARNING_BUFFER_SIZE){
			LOG.warn("buffer size is above {} it's {}",WARNING_BUFFER_SIZE, buffer.size());
		}
		if (buffer.size() >= MAX_BUFFER_SIZE) {
			LOG.warn("Event {} was removed from the queue due to insufficient space", buffer.poll().getTaskId());
		}
		try {
			buffer.add(e);
		} catch (RuntimeException ex) {
			// ignore
			LOG.warn("Failed adding event to buffer", ex);
		}
	}

	@Override
	public void close() {
		eventOutputPipe.close();
	}

	@Override
	public Map<String, String> getStaticParams() {
		return eventOutputPipe.getStaticParams();
	}

	public int getCurrentBufferSize() {
		return buffer.size();
	}
}
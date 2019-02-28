package com.datorama.timbermill.pipe;

import com.datorama.timbermill.unit.Event;

public class BlackHolePipe implements EventOutputPipe {

	@Override public void send(Event e) {
		//Do nothing
	}

	@Override public int getMaxQueueSize() {
		return 0;
	}

	@Override
	public void close() {

	}

}
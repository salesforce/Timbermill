package com.datorama.oss.timbermill.pipe;

import com.datorama.oss.timbermill.unit.Event;

public class BlackHolePipe implements EventOutputPipe {

	@Override public void send(Event e) {
		//Do nothing
	}

	@Override public int getCurrentBufferSize() {
		return 0;
	}

}
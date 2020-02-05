package com.datorama.timbermill;

import com.datorama.timbermill.pipe.EventOutputPipe;
import com.datorama.timbermill.unit.Event;

class BlackHolePipe implements EventOutputPipe {

	@Override public void send(Event e) {
		//Do nothing
	}

	@Override public int getCurrentBufferSize() {
		return 0;
	}

}
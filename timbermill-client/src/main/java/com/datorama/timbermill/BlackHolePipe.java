package com.datorama.timbermill;

public class BlackHolePipe implements EventOutputPipe {

	@Override public void send(Event e) {
		//Do nothing
	}

	@Override public int getMaxQueueSize() {
		return 0;
	}

}
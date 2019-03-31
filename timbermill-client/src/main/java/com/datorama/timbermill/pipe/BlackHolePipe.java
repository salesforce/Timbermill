package com.datorama.timbermill.pipe;

import com.datorama.timbermill.unit.Event;

import java.util.Collections;
import java.util.Map;

public class BlackHolePipe implements EventOutputPipe {

	@Override public void send(Event e) {
		//Do nothing
	}

	@Override
	public void close() {

	}

	@Override
	public Map<String, String> getStaticParams() {
		return Collections.EMPTY_MAP;
	}

}
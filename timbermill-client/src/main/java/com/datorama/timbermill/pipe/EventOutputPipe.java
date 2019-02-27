package com.datorama.timbermill.pipe;

import com.datorama.timbermill.unit.Event;

public interface EventOutputPipe {

    void send(Event e);

    int getMaxQueueSize();

}

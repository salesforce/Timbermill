package com.datorama.timbermill.pipe;

import com.datorama.timbermill.Event;

public interface EventOutputPipe {

    void send(Event e);

    int getMaxQueueSize();

}

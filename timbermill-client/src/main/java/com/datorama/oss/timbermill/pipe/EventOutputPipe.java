package com.datorama.oss.timbermill.pipe;

import com.datorama.oss.timbermill.unit.Event;

public interface EventOutputPipe {

    void send(Event e);

    int getCurrentBufferSize();
}

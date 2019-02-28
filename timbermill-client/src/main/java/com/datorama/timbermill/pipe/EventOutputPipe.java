package com.datorama.timbermill.pipe;

import com.datorama.timbermill.unit.Event;

import java.io.IOException;

public interface EventOutputPipe {

    void send(Event e);

    int getMaxQueueSize();

    void close() throws IOException;
}

package com.datorama.timbermill;

public interface EventOutputPipe {

    void send(Event e);

    int getMaxQueueSize();

}

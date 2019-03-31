package com.datorama.timbermill.pipe;

import com.datorama.timbermill.unit.Event;

import java.util.Map;

public interface EventOutputPipe {

    void send(Event e);

    void close();

    Map<String, String> getStaticParams();
}

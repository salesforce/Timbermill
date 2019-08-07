package com.datorama.timbermill.pipe;

import com.datorama.timbermill.ElasticsearchClient;
import com.datorama.timbermill.unit.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

public class TimbermillServerOutputPipe implements EventOutputPipe {

    private static final Logger LOG = LoggerFactory.getLogger(TimbermillServerOutputPipe.class);

    public TimbermillServerOutputPipe() {
    }

    @Override
    public void send(Event e) {

    }

    @Override
    public void close() {
    }

    @Override
    public Map<String, String> getStaticParams() {
        return null;
    }

}
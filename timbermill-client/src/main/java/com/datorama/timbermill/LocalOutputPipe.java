package com.datorama.timbermill;

import com.datorama.timbermill.pipe.EventOutputPipe;
import com.datorama.timbermill.pipe.LocalTaskIndexer;

import java.util.Collections;
import java.util.Map;

public class LocalOutputPipe implements EventOutputPipe {

    private LocalTaskIndexer localTaskIndexer;

    public LocalOutputPipe() {
        String pluginsJson = "[]";
        String env = "local";
        Map emptyMap = Collections.EMPTY_MAP;
        int defaultMaxChars = 10000;
        String elasticUrl = "localhost";
        int elasticPort = 9200;
        String elasticScheme = "http";
        int indexBulkSize = 10000;
        int daysBackToDelete = 90;
        ElasticsearchClient es = new ElasticsearchClient(env, elasticUrl, elasticPort, elasticScheme, indexBulkSize, daysBackToDelete);
        localTaskIndexer = new LocalTaskIndexer(pluginsJson, env, emptyMap, defaultMaxChars, es);
    }

    @Override
    public void send(Event e){
        localTaskIndexer.addEvent(e);
    }

    @Override
    public int getMaxQueueSize() {
        return 0;
    }
}

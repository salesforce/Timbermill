package com.datorama.timbermill.pipe;

import com.datorama.timbermill.ElasticsearchClient;
import com.datorama.timbermill.unit.Event;
import com.datorama.timbermill.LocalTaskIndexer;

public class LocalOutputPipe implements EventOutputPipe {

    private LocalTaskIndexer localTaskIndexer;

    public LocalOutputPipe(LocalOutputPipeConfig config) {
        ElasticsearchClient es = new ElasticsearchClient(config.getEnv(), config.getElasticUrl(), config.getIndexBulkSize(), config.getDaysBackToDelete());
        localTaskIndexer = new LocalTaskIndexer(config.getPlugingJson(), config.getPropertiesLengthMap(), config.getDefaultMaxChars(), es);
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

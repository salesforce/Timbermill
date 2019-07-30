package com.datorama.timbermill.pipe;

import com.datorama.timbermill.ElasticsearchClient;
import com.datorama.timbermill.LocalTaskIndexer;
import com.datorama.timbermill.unit.Event;

import java.util.Map;

public class LocalOutputPipe implements EventOutputPipe {

    private LocalTaskIndexer localTaskIndexer;
    private Map<String, String> staticParams;

    public LocalOutputPipe(LocalOutputPipeConfig config) {
        ElasticsearchClient es = new ElasticsearchClient(config.getEnv(), config.getElasticUrl(), config.getIndexBulkSize(), config.getDaysBackToDelete(), config.getAwsRegion());
        localTaskIndexer = new LocalTaskIndexer(config.getPlugingJson(), config.getPropertiesLengthMap(), config.getDefaultMaxChars(), es, config.getSecondBetweenPolling(), config.getMaxEventsToDrainFromQueue());
        staticParams = config.getStaticParams();
    }

    @Override
    public void send(Event e){
        localTaskIndexer.addEvent(e);
    }

    public void close() {
        localTaskIndexer.close();
    }

    @Override
    public Map<String, String> getStaticParams() {
        return staticParams;
    }
}

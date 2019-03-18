package com.datorama.timbermill.pipe;

import org.elasticsearch.ElasticsearchException;
import java.util.Collections;
import java.util.Map;

public class LocalOutputPipeConfig {

    private final int daysBackToDelete;
    private final int defaultMaxChars;
    private final String elasticUrl;
    private final String env;
    private final String plugingJson;
    private final Map<String, Integer> propertiesLengthMap;
    private final int indexBulkSize;
    private final Map<String, String> staticParams;
    private int secondBetweenPolling;

    public LocalOutputPipeConfig(Builder builder) {
        if (builder == null){
            throw new ElasticsearchException("LocalOutputPipeConfig.Builder can't be null");
        }
        if (builder.elasticUrl == null){
            throw new ElasticsearchException("Must enclose an Elasticsearch URL");
        }
        elasticUrl = builder.elasticUrl;
        env = builder.env;
        plugingJson = builder.plugingJson;
        propertiesLengthMap = builder.propertiesLengthMap;
        defaultMaxChars = builder.defaultMaxChars;
        indexBulkSize = builder.indexBulkSize;
        daysBackToDelete = builder.daysBackToDelete;
        staticParams = builder.staticParams;
        secondBetweenPolling = builder.secondBetweenPolling;
    }

    int getDaysBackToDelete() {
        return daysBackToDelete;
    }

    int getDefaultMaxChars() {
        return defaultMaxChars;
    }

    String getElasticUrl() {
        return elasticUrl;
    }

    String getEnv() {
        return env;
    }

    String getPlugingJson() {
        return plugingJson;
    }

    Map<String, Integer> getPropertiesLengthMap() {
        return propertiesLengthMap;
    }

    int getIndexBulkSize() {
        return indexBulkSize;
    }

    int getSecondBetweenPolling() {
        return secondBetweenPolling;
    }

    public Map<String, String> getStaticParams() {
        return staticParams;
    }

    public static class Builder {
        private String elasticUrl = null;
        private String env = "default";
        private String plugingJson = "[]";
        private Map<String, Integer> propertiesLengthMap = Collections.EMPTY_MAP;
        private int defaultMaxChars = 1000000;
        private int indexBulkSize = 1000;
        private int daysBackToDelete = 0;
        private int secondBetweenPolling = 10;
        private Map<String, String> staticParams = Collections.EMPTY_MAP;

        public Builder url(String elasticUrl) {
            this.elasticUrl = elasticUrl;
            return this;
        }

        public Builder env(String env) {
            this.env = env;
            return this;
        }

        public Builder pluginJson(String plugingJson) {
            this.plugingJson = plugingJson;
            return this;
        }

        public Builder propertiesLengthMap(Map<String, Integer> propertiesLengthMap) {
            this.propertiesLengthMap = propertiesLengthMap;
            return this;
        }

        public Builder defaultMaxChars(int defaultMaxChars) {
            this.defaultMaxChars = defaultMaxChars;
            return this;
        }

        public Builder indexBulkSize(int indexBulkSize) {
            this.indexBulkSize = indexBulkSize;
            return this;
        }

        public Builder daysBackToDelete(int daysBackToDelete) {
            this.daysBackToDelete = daysBackToDelete;
            return this;
        }

        public Builder staticParams(Map<String, String> staticParams) {
            this.staticParams = staticParams;
            return this;
        }

        public Builder secondBetweenPolling(int secondBetweenPolling) {
            this.secondBetweenPolling = secondBetweenPolling;
            return this;
        }

        public LocalOutputPipeConfig build() {
            return new LocalOutputPipeConfig(this);
        }
    }
}

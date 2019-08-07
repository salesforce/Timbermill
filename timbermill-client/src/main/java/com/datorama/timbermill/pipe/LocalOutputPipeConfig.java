package com.datorama.timbermill.pipe;

import com.google.common.collect.Maps;
import org.elasticsearch.ElasticsearchException;
import java.util.Collections;
import java.util.Map;

import static com.datorama.timbermill.TimberLogger.ENV;

public class LocalOutputPipeConfig {

    private final int daysRotation;
    private final int defaultMaxChars;
    private final String elasticUrl;
    private final String plugingJson;
    private final Map<String, Integer> propertiesLengthMap;
    private final Map<String, String> staticParams;
    private int secondBetweenPolling;
    private String awsRegion;
    private int indexBulkSize;

    private LocalOutputPipeConfig(Builder builder) {
        if (builder == null){
            throw new ElasticsearchException("LocalOutputPipeConfig.Builder can't be null");
        }
        if (builder.elasticUrl == null){
            throw new ElasticsearchException("Must enclose an Elasticsearch URL");
        }
        elasticUrl = builder.elasticUrl;
        plugingJson = builder.plugingJson;
        propertiesLengthMap = builder.propertiesLengthMap;
        defaultMaxChars = builder.defaultMaxChars;
        daysRotation = builder.daysRotation;
        staticParams = builder.staticParams;
        secondBetweenPolling = builder.secondBetweenPolling;
        staticParams.put(ENV, builder.env);
        awsRegion = builder.awsRegion;
        indexBulkSize = builder.indexBulkSize;
    }

    public int getDaysRotation() {
        return daysRotation;
    }

    public int getDefaultMaxChars() {
        return defaultMaxChars;
    }

    public String getElasticUrl() {
        return elasticUrl;
    }

    public String getEnv() {
        return staticParams.get(ENV);
    }

    public String getPlugingJson() {
        return plugingJson;
    }

    public Map<String, Integer> getPropertiesLengthMap() {
        return propertiesLengthMap;
    }

    int getSecondBetweenPolling() {
        return secondBetweenPolling;
    }

    Map<String, String> getStaticParams() {
        return staticParams;
    }

    public String getAwsRegion() {
        return awsRegion;
    }

    public int getIndexBulkSize() {
        return indexBulkSize;
    }

    public static class Builder {
        private String awsRegion;
        private String elasticUrl = null;
        private String env = "default";
        private String plugingJson = "[]";
        private Map<String, Integer> propertiesLengthMap = Collections.EMPTY_MAP;
        private int defaultMaxChars = 1000000;
        private int daysRotation = 0;
        private int secondBetweenPolling = 10;
        private int indexBulkSize = 1000;
        private Map<String, String> staticParams = Maps.newHashMap();

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

        public Builder daysRotation(int daysRotation) {
            this.daysRotation = daysRotation;
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

        public Builder awsRegion(String awsRegion) {
            this.awsRegion = awsRegion;
            return this;
        }

        public Builder indexBulkSize(int indexBulkSize) {
            this.indexBulkSize = indexBulkSize;
            return this;
        }

        public LocalOutputPipeConfig build() {
            return new LocalOutputPipeConfig(this);
        }
    }
}

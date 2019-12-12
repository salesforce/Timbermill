package com.datorama.timbermill;

import java.util.Map;

public class ElasticsearchParams {
	private final String pluginsJson;
	private final Map<String, Integer> propertiesLengthJson;
	private final int defaultMaxChars;
	private final String elasticUrl;
	private final int daysRotation;
	private final String awsRegion;
	private final int indexBulkSize;
	private final int indexingThreads;
	private final String elasticUser;
	private final String elasticPassword;
	private final int maximumCacheSize;
	private final int maximumCacheMinutesHold;

	public ElasticsearchParams(String elasticUrl, int defaultMaxChars, int daysRotation, String awsRegion, int indexingThreads, String elasticUser, String elasticPassword, int indexBulkSize,
			String pluginsJson,
			Map<String, Integer> propertiesLengthJson,
			int maximumCacheSize, int maximumCacheMinutesHold) {
		this.pluginsJson = pluginsJson;
		this.propertiesLengthJson = propertiesLengthJson;
		this.defaultMaxChars = defaultMaxChars;
		this.elasticUrl = elasticUrl;
		this.daysRotation = daysRotation;
		this.awsRegion = awsRegion;
		this.indexBulkSize = indexBulkSize;
		this.indexingThreads = indexingThreads;
		this.elasticUser = elasticUser;
		this.elasticPassword = elasticPassword;
		this.maximumCacheSize = maximumCacheSize;
		this.maximumCacheMinutesHold = maximumCacheMinutesHold;
	}

	String getPluginsJson() {
		return pluginsJson;
	}

	Map<String, Integer> getPropertiesLengthJson() {
		return propertiesLengthJson;
	}

	int getDefaultMaxChars() {
		return defaultMaxChars;
	}

	String getElasticUrl() {
		return elasticUrl;
	}

	int getDaysRotation() {
		return daysRotation;
	}

	String getAwsRegion() {
		return awsRegion;
	}

	int getIndexBulkSize() {
		return indexBulkSize;
	}

	int getIndexingThreads() {
		return indexingThreads;
	}

	String getElasticUser() {
		return elasticUser;
	}

	String getElasticPassword() {
		return elasticPassword;
	}

	int getMaximumCacheSize() {
		return maximumCacheSize;
	}

	int getMaximumCacheMinutesHold() {
		return maximumCacheMinutesHold;
	}
}

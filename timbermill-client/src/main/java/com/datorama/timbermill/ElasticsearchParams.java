package com.datorama.timbermill;

import java.util.Map;

public class ElasticsearchParams {
	private final String pluginsJson;
	private final Map<String, Integer> propertiesLengthJson;
	private final int defaultMaxChars;
	private final int maximumCacheSize;
	private final int maximumCacheMinutesHold;
	private int numberOfShards;
	private int numberOfReplicas;
	private int daysRotation;

	public ElasticsearchParams(int defaultMaxChars, String pluginsJson, Map<String, Integer> propertiesLengthJson, int maximumCacheSize, int maximumCacheMinutesHold, int numberOfShards,
			int numberOfReplicas, int daysRotation) {
		this.pluginsJson = pluginsJson;
		this.propertiesLengthJson = propertiesLengthJson;
		this.defaultMaxChars = defaultMaxChars;
		this.maximumCacheSize = maximumCacheSize;
		this.maximumCacheMinutesHold = maximumCacheMinutesHold;
		this.numberOfShards = numberOfShards;
		this.numberOfReplicas = numberOfReplicas;
		this.daysRotation = daysRotation;
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

	int getMaximumCacheSize() {
		return maximumCacheSize;
	}

	int getMaximumCacheMinutesHold() {
		return maximumCacheMinutesHold;
	}

	public int getNumberOfShards() {
		return numberOfShards;
	}

	public int getNumberOfReplicas() {
		return numberOfReplicas;
	}

	public int getDaysRotation() {
		return daysRotation;
	}
}

package com.datorama.oss.timbermill;

public class ElasticsearchParams {
	private final String pluginsJson;
	private final int maximumCacheSize;
	private final int maximumCacheMinutesHold;
	private int numberOfShards;
	private int numberOfReplicas;
	private int daysRotation;
	private String deletionCronExp;
	private String mergingCronExp;
	private int maxTotalFields;

	public ElasticsearchParams(String pluginsJson, int maximumCacheSize, int maximumCacheMinutesHold, int numberOfShards,
			int numberOfReplicas, int daysRotation, String deletionCronExp, String mergingCronExp, int maxTotalFields) {
		this.pluginsJson = pluginsJson;
		this.maximumCacheSize = maximumCacheSize;
		this.maximumCacheMinutesHold = maximumCacheMinutesHold;
		this.numberOfShards = numberOfShards;
		this.numberOfReplicas = numberOfReplicas;
		this.daysRotation = daysRotation;
		this.deletionCronExp = deletionCronExp;
		this.mergingCronExp = mergingCronExp;
		this.maxTotalFields = maxTotalFields;
	}

	public int getNumberOfShards() {
		return numberOfShards;
	}

	public int getNumberOfReplicas() {
		return numberOfReplicas;
	}

	public String getDeletionCronExp() {
		return deletionCronExp;
	}

	public String getMergingCronExp() {
		return mergingCronExp;
	}

	public int getMaxTotalFields() {
		return maxTotalFields;
	}

	String getPluginsJson() {
		return pluginsJson;
	}

	int getMaximumCacheSize() {
		return maximumCacheSize;
	}

	int getMaximumCacheMinutesHold() {
		return maximumCacheMinutesHold;
	}

	int getDaysRotation() {
		return daysRotation;
	}
}

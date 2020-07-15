package com.datorama.oss.timbermill;

public class ElasticsearchParams {
	private final String pluginsJson;
	private final int maximumCacheSize;
	private final int maximumCacheSecondsHold;
	private int numberOfShards;
	private int numberOfReplicas;
	private int daysRotation;
	private String deletionCronExp;
	private String mergingCronExp;
	private int maxTotalFields;
	private String persistentFetchCronExp;

	public ElasticsearchParams(String pluginsJson, int maximumCacheSize, int maximumCacheSecondsHold, int numberOfShards,
			int numberOfReplicas, int daysRotation, String deletionCronExp, String mergingCronExp, int maxTotalFields, String persistentFetchCronExp) {
		this.pluginsJson = pluginsJson;
		this.maximumCacheSize = maximumCacheSize;
		this.maximumCacheSecondsHold = maximumCacheSecondsHold;
		this.numberOfShards = numberOfShards;
		this.numberOfReplicas = numberOfReplicas;
		this.daysRotation = daysRotation;
		this.deletionCronExp = deletionCronExp;
		this.mergingCronExp = mergingCronExp;
		this.maxTotalFields = maxTotalFields;
		this.persistentFetchCronExp = persistentFetchCronExp;
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

	int getMaximumCacheSecondsHold() {
		return maximumCacheSecondsHold;
	}

	int getDaysRotation() {
		return daysRotation;
	}

	public String getPersistentFetchCronExp() { return persistentFetchCronExp; }

}

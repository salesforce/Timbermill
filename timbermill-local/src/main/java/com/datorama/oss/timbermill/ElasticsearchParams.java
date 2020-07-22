package com.datorama.oss.timbermill;

public class ElasticsearchParams {
	private final String pluginsJson;
	private int numberOfShards;
	private int numberOfReplicas;
	private int daysRotation;
	private String deletionCronExp;
	private String mergingCronExp;
	private int maxTotalFields;
	private String persistentFetchCronExp;
	private String parentsFetchCronExp;
	private int orphansFetchPeriodMinutes;

	public ElasticsearchParams(String pluginsJson, int numberOfShards,
			int numberOfReplicas, int daysRotation, String deletionCronExp, String mergingCronExp, int maxTotalFields,
			String persistentFetchCronExp, String parentsFetchCronExp, int orphansFetchPeriodMinutes) {
		this.pluginsJson = pluginsJson;
		this.numberOfShards = numberOfShards;
		this.numberOfReplicas = numberOfReplicas;
		this.daysRotation = daysRotation;
		this.deletionCronExp = deletionCronExp;
		this.mergingCronExp = mergingCronExp;
		this.maxTotalFields = maxTotalFields;
		this.persistentFetchCronExp = persistentFetchCronExp;
		this.parentsFetchCronExp = parentsFetchCronExp;
		this.orphansFetchPeriodMinutes = orphansFetchPeriodMinutes;
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

	public int getDaysRotation() { return daysRotation; }

	public String getPersistentFetchCronExp() { return persistentFetchCronExp; }

	public String getParentsFetchCronExp() {  return parentsFetchCronExp; }

	public int getOrphansFetchPeriodMinutes() { return orphansFetchPeriodMinutes;}

}

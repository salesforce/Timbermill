package com.datorama.oss.timbermill.pipe;

public class TimbermillServerOutputPipeBuilder {
	String timbermillServerUrl;
	int maxEventsBatchSize = 2097152; // 2MB
	long maxSecondsBeforeBatchTimeout = 3;
	int maxBufferSize = 200000;
	int numOfThreads = 3;


	public TimbermillServerOutputPipeBuilder timbermillServerUrl(String timbermillServerUrl) {
		this.timbermillServerUrl = timbermillServerUrl;
		return this;
	}

	public TimbermillServerOutputPipeBuilder maxEventsBatchSize(int maxEventsBatchSize) {
		this.maxEventsBatchSize = maxEventsBatchSize;
		return this;
	}

	public TimbermillServerOutputPipeBuilder maxSecondsBeforeBatchTimeout(long maxSecondsBeforeBatchTimeout) {
		this.maxSecondsBeforeBatchTimeout = maxSecondsBeforeBatchTimeout;
		return this;
	}

	public TimbermillServerOutputPipeBuilder maxBufferSize(int maxBufferSize) {
		this.maxBufferSize = maxBufferSize;
		return this;
	}

	public TimbermillServerOutputPipeBuilder numOfThreads(int numOfThreads) {
		this.numOfThreads = numOfThreads;
		return this;
	}

	public TimbermillClient build() {
		return new TimbermillClient(this);
	}

}

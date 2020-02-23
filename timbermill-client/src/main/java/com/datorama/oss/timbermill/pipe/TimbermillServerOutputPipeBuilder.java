package com.datorama.oss.timbermill.pipe;

import com.datorama.oss.timbermill.common.Constants;

public class TimbermillServerOutputPipeBuilder {
	String timbermillServerUrl;
	int maxEventsBatchSize = 2097152; // 2MB
	long maxSecondsBeforeBatchTimeout = 3;
	int maxBufferSize = 200000;
	int maxCharsAllowedForNonAnalyzedFields = Constants.MAX_CHARS_ALLOWED_FOR_NON_ANALYZED_FIELDS;
	int maxCharsAllowedForAnalyzedFields = Constants.MAX_CHARS_ALLOWED_FOR_ANALYZED_FIELDS;


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

	public TimbermillServerOutputPipeBuilder maxCharsAllowedForNonAnalyzedFields(int maxCharsAllowedForNonAnalyzedFields) {
		if (maxCharsAllowedForNonAnalyzedFields > 0 && maxCharsAllowedForNonAnalyzedFields < Constants.MAX_CHARS_ALLOWED_FOR_NON_ANALYZED_FIELDS) {
			this.maxCharsAllowedForNonAnalyzedFields = maxCharsAllowedForNonAnalyzedFields;
		}
		return this;
	}

	public TimbermillServerOutputPipeBuilder maxCharsAllowedForAnalyzedFields(int maxCharsAllowedForAnalyzedFields) {
		if (maxCharsAllowedForAnalyzedFields > 0 && maxCharsAllowedForAnalyzedFields < Constants.MAX_CHARS_ALLOWED_FOR_ANALYZED_FIELDS) {
			this.maxCharsAllowedForAnalyzedFields = maxCharsAllowedForAnalyzedFields;
		}
		return this;
	}

	public TimbermillServerOutputPipe build() {
		return new TimbermillServerOutputPipe(this);
	}

}

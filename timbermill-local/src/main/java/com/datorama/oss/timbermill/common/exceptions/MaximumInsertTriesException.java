package com.datorama.oss.timbermill.common.exceptions;

public class MaximumInsertTriesException extends Exception {
	private int maxTries;
	public MaximumInsertTriesException(int maxTries) {
		this.maxTries = maxTries;
	}

	public int getMaximumTriesNumber(){
		return maxTries;
	}
}

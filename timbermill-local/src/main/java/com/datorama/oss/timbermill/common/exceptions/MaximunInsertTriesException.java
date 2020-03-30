package com.datorama.oss.timbermill.common.exceptions;

public class MaximunInsertTriesException extends Exception {
	private int maxTries;
	public MaximunInsertTriesException(int maxTries) {
		this.maxTries = maxTries;
	}

	public int getMaximumTriesNumber(){
		return maxTries;
	}
}

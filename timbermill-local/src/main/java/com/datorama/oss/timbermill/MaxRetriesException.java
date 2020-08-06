package com.datorama.oss.timbermill;

class MaxRetriesException extends Exception {
	public MaxRetriesException(Throwable cause) {
		super(cause);
	}
}

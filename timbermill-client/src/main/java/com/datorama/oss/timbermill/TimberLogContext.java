package com.datorama.oss.timbermill;

public class TimberLogContext implements AutoCloseable{

	private final String ongoingTaskId;

	public TimberLogContext(String ongoingTaskId) {
		this.ongoingTaskId = ongoingTaskId;
		if (ongoingTaskId != null) {
			EventLogger.get().addIdToContext(ongoingTaskId);
		}
	}

	@Override public void close(){
		if (ongoingTaskId != null) {
			EventLogger.get().removeIdFromContext(ongoingTaskId);
		}
	}
}

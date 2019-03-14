package com.datorama.timbermill;

public class TimberLogContext implements AutoCloseable{

	private String ongoingTaskId;

	TimberLogContext(String ongoingTaskId) {
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

	public String getOngoingTaskId() {
		return ongoingTaskId;
	}
}

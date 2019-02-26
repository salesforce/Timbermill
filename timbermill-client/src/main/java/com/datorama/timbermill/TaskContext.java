package com.datorama.timbermill;

public class TaskContext implements AutoCloseable{

	private String ongoingTaskId;

	TaskContext(String ongoingTaskId) {
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

package com.datorama.oss.timbermill.unit;

import java.time.ZonedDateTime;

public class AdoptedEvent extends Event {

	private String index;

	public AdoptedEvent(String taskId, Task t) {
		this.setOrphan(false);
		this.name = t.getName();
		this.setEnv(t.getEnv());
		this.taskId = taskId;
		this.setParentId(t.getParentId());
		this.setContext(t.getCtx());
		this.index = t.getIndex();
	}

	@Override public TaskStatus getStatusFromExistingStatus(TaskStatus status, ZonedDateTime startTime, ZonedDateTime taskEndTime, String taskParentId, String taskName) {
		return null;
	}

	@Override public boolean isAdoptedEvent(){
		return true;
	}

	@Override public String getIndex() { return index; }
}

package com.datorama.oss.timbermill.unit;

import java.util.Map;

public class AdoptedEvent extends Event {

	public AdoptedEvent(String taskId, Task t) {
		this.setOrphan(false);
		this.name = t.getName();
		this.setEnv(t.getEnv());
		this.taskId = taskId;
		this.setParentId(t.getParentId());
		this.setContext(t.getCtx());
	}

	public AdoptedEvent(Map.Entry<String, Task> entry) {
		this(entry.getKey(), entry.getValue());

	}

	@Override public TaskStatus getStatusFromExistingStatus(TaskStatus status) {
		return null;
	}

	@Override public boolean isAdoptedEvent(){
		return true;
	}
}

package com.datorama.oss.timbermill.unit;

public class AdoptedEvent extends Event {

	public AdoptedEvent(String taskId, Task t) {
		this.setOrphan(false);
		this.name = t.getName();
		this.setEnv(t.getEnv());
		this.taskId = taskId;
		this.setParentId(t.getParentId());
		this.setContext(t.getCtx());
	}

	@Override public TaskStatus getStatusFromExistingStatus(TaskStatus status) {
		return null;
	}

	@Override public boolean isAdoptedEvent(){
		return true;
	}
}

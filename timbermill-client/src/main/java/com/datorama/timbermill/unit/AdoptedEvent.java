package com.datorama.timbermill.unit;

public class AdoptedEvent extends Event {

	public AdoptedEvent(Event e) {
		this.setOrphan(false);
		this.name = e.getName();
		this.taskId = e.getTaskId();
		this.setParentId(e.getParentId());
		this.setContext(e.getContext());
	}

	@Override public Task.TaskStatus getStatusFromExistingStatus(Task.TaskStatus status) {
		return null;
	}
}

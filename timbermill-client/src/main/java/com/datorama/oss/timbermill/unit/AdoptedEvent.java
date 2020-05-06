package com.datorama.oss.timbermill.unit;

public class AdoptedEvent extends Event {

	public AdoptedEvent(Event e) {
		this.setOrphan(false);
		this.name = e.getName();
		this.taskId = e.getTaskId();
		this.setParentId(e.getParentId());
		this.setContext(e.getContext());
	}

	@Override public TaskStatus getStatusFromExistingStatus(TaskStatus status) {
		return null;
	}
}

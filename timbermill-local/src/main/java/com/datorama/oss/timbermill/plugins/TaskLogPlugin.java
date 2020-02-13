package com.datorama.oss.timbermill.plugins;

import java.io.Serializable;
import java.util.Collection;

import com.datorama.oss.timbermill.unit.Event;

public abstract class TaskLogPlugin implements Serializable{

	private String name;

	public TaskLogPlugin() {
	}

	TaskLogPlugin(String name) {
		this.name = name;
	}

	public abstract void apply (Collection<Event> events);

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override public String toString() {
		return name;
	}
}

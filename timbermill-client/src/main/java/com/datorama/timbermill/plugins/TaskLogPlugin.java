package com.datorama.timbermill.plugins;

import com.datorama.timbermill.unit.Event;

import java.io.Serializable;
import java.util.List;

public abstract class TaskLogPlugin implements Serializable{

	private String name;

	public TaskLogPlugin() {
	}

	TaskLogPlugin(String name) {
		this.name = name;
	}

	public abstract void apply (List<Event> events);

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

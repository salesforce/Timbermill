package com.datorama.timbermill.plugins;

import com.datorama.timbermill.unit.Event;
import com.datorama.timbermill.unit.Task;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public abstract class TaskLogPlugin implements Serializable{

	private String name;

	public TaskLogPlugin() {
	}

	public TaskLogPlugin(String name) {
		this.name = name;
	}

	public abstract void apply (List<Event> events, Map<String, Task> tasks);

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

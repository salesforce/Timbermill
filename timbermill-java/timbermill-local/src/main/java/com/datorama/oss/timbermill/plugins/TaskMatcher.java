package com.datorama.oss.timbermill.plugins;

import java.io.Serializable;

import com.datorama.oss.timbermill.unit.Event;
import com.datorama.oss.timbermill.unit.Task;

public class TaskMatcher implements Serializable{
	private String name;

	public TaskMatcher(String name) {
		if (name == null){
			throw new NullPointerException("name in TaskMatcher can't be null");
		}
		this.name = name;
	}

	boolean matches(Event e) {
		String name = Task.getNameFromId(e.getName(), e.getTaskId());
		return this.name.equals(name);
	}

}

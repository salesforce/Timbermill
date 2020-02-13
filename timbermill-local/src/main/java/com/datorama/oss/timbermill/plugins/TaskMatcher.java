package com.datorama.oss.timbermill.plugins;

import java.io.Serializable;

import com.datorama.oss.timbermill.unit.Event;

public class TaskMatcher implements Serializable{
	private String name;

	public TaskMatcher(String name) {
		if (name == null){
			throw new NullPointerException("name in TaskMatcher can't be null");
		}
		this.name = name;
	}

	boolean matches(Event e) {
		String name = e.getNameFromId();
		return this.name.equals(name);
	}

}

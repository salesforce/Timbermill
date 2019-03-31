package com.datorama.timbermill.plugins;

import com.datorama.timbermill.unit.Event;

import java.io.Serializable;

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

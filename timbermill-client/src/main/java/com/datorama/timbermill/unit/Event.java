package com.datorama.timbermill.unit;

import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class Event{

	private String name;
	private EventType eventType;
	private String taskId;
	private String parentId;
	private String primaryId;
	private Map<String, Object> strings = new HashMap<>();
	private Map<String, String> texts = new HashMap<>();
	private Map<String, Object> globals = new HashMap<>();
	private Map<String, Number> metrics = new HashMap<>();
	private DateTime time;

	public Event(String taskId, EventType eventType, DateTime time) {
        this(taskId, eventType, time, null);
    }

    public Event(String taskId, EventType eventType, DateTime time,String name) {
		this.taskId = taskId;
		this.eventType = eventType;
		this.time = time;
		this.name = name;
	}

	private static final int MAX_CHARS = 1000000;

    public String getTaskId() {
        return taskId;
    }

    public String getName() {
        return name;
    }

    public EventType getEventType() {
        return eventType;
    }

    void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getParentId() {
        return parentId;
    }

    public void setParentId(String parentId) {
        this.parentId = parentId;
    }

    public String getPrimaryId() {
        return primaryId;
    }

    public void setPrimaryId(String primaryId) {
        this.primaryId = primaryId;
    }

    public DateTime getTime() {
        return time;
    }

	public Map<String, Object> getStrings() {
        return strings;
    }

	public Map<String, Number> getMetrics() {
		return metrics;
	}

	public Map<String, String> getTexts() {
		return texts;
	}

	public void setStrings(Map<String, ?> strings) {
		if(strings != null) {
			for (Entry<String, ?> entry : strings.entrySet()){
				String value = String.valueOf(entry.getValue());
				if (value == null){
					value = "null";
				}
				this.strings.put(entry.getKey(), value.substring(0, Math.min(value.length(), MAX_CHARS))); // Preventing ultra big strings
			}
		}
	}

	public void setMetrics(Map<String, Number> metric) {
		if(metric != null) {
			this.metrics.putAll(metric);
		}
	}

	public void setTexts(Map<String, String> text) {
		if (text != null) {
			for (Entry<String, String> entry : text.entrySet()){
				String value = entry.getValue();
				if (value == null){
					value = "null";
				}
				this.texts.put(entry.getKey(), value.substring(0, Math.min(value.length(), MAX_CHARS))); // Preventing ultra big strings
			}
		}
	}

	public Map<String, Object> getGlobals() {
		return globals;
	}

	public void setGlobals(Map<String, ?> global) {
		if(global != null) {
			for (Entry<String, ?> entry : global.entrySet()){
				String value = String.valueOf(entry.getValue());
				if (value == null){
					value = "null";
				}
				this.globals.put(entry.getKey(), value.substring(0, Math.min(value.length(), MAX_CHARS))); // Preventing ultra big strings
			}
		}
	}

	public enum EventType {
		START,
		END_SUCCESS,
		END_ERROR,
		INFO,
		SPOT
	}
}

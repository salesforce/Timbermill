package com.datorama.timbermill.unit;

import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class Event{

	private String taskType;
	private EventType eventType;
	private String taskId;
	private String parentTaskId;
	private String primaryTaskId;
	private Map<String, Object> attributes = new HashMap<>();
	private Map<String, Number> metrics = new HashMap<>();
	private Map<String, String> data = new HashMap<>();
	private DateTime time;

    public Event(String taskId, EventType eventType, DateTime time) {
        this(taskId, eventType, time, null);
    }

    public Event(String taskId, EventType eventType, DateTime time,String taskType) {
		this.taskId = taskId;
		this.eventType = eventType;
		this.time = time;
		this.taskType = taskType;
	}

	private static final int MAX_CHARS = 1000000;

    public String getTaskId() {
        return taskId;
    }

    public String getTaskType() {
        return taskType;
    }

    public EventType getEventType() {
        return eventType;
    }

    void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getParentTaskId() {
        return parentTaskId;
    }

    public void setParentTaskId(String parentTaskId) {
        this.parentTaskId = parentTaskId;
    }

    public String getPrimaryTaskId() {
        return primaryTaskId;
    }

    public void setPrimaryTaskId(String primaryTaskId) {
        this.primaryTaskId = primaryTaskId;
    }

    public DateTime getTime() {
        return time;
    }

	public Map<String, Object> getAttributes() {
        return attributes;
    }

	public Map<String, Number> getMetrics() {
		return metrics;
	}

	public Map<String, String> getData() {
		return data;
	}

	public void setAttributes(Map<String, ?> attributes) {
		if(attributes != null) {
			for (Entry<String, ?> entry : attributes.entrySet()){
				String value = String.valueOf(entry.getValue());
				if (value == null){
					value = "null";
				}
				this.attributes.put(entry.getKey(), value.substring(0, Math.min(value.length(), MAX_CHARS))); // Preventing ultra big strings
			}
		}
	}

	public void setMetrics(Map<String, Number> metrics) {
		if(metrics != null) {
			this.metrics.putAll(metrics);
		}
	}

	public void setData(Map<String, String> data) {
		if (data != null) {
			for (Entry<String, String> entry : data.entrySet()){
				String value = entry.getValue();
				if (value == null){
					value = "null";
				}
				this.data.put(entry.getKey(), value.substring(0, Math.min(value.length(), MAX_CHARS))); // Preventing ultra big strings
			}
		}
	}

	public enum EventType {
		START,
		END_SUCCESS,
		END_ERROR,
		END_APP_ERROR,
		INFO,
		SPOT
	}
}

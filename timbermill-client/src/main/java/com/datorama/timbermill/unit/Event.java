package com.datorama.timbermill.unit;

import org.joda.time.DateTime;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class Event{

	private String name;
	private EventType eventType;
	private String taskId;
	private String parentId;
	private String primaryId;
	private Map<String, String> strings = new HashMap<>();
	private Map<String, String> texts = new HashMap<>();
	private Map<String, String> globals = new HashMap<>();
	private Map<String, Number> metrics = new HashMap<>();
	private DateTime time;

    public Event(@NotNull String taskId, EventType eventType, String name) {
		this.taskId = taskId;
		this.eventType = eventType;
		this.time = new DateTime();
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

    DateTime getTime() {
        return time;
    }

	public Map<String, String> getStrings() {
        return strings;
    }

	public Map<String, Number> getMetrics() {
		return metrics;
	}

	public Map<String, String> getTexts() {
		return texts;
	}

	public Map<String, String> getGlobals() {
		return globals;
	}

	public void setStrings(@NotNull Map<String, String> strings) {
		for (Entry<String, String> entry : strings.entrySet()){
			String trimmedString = getTrimmedString(entry.getValue());
			this.strings.put(entry.getKey(), trimmedString); // Preventing ultra big strings
		}
	}

	public void setTexts(@NotNull Map<String, String> text) {
		for (Entry<String, String> entry : text.entrySet()){
			String trimmedString = getTrimmedString(entry.getValue());
			this.texts.put(entry.getKey(), trimmedString); // Preventing ultra big strings
		}
	}

	public void setGlobals(@NotNull Map<String, String> global) {
		for (Entry<String, String> entry : global.entrySet()){
			String trimmedString = getTrimmedString(entry.getValue());
			this.globals.put(entry.getKey(), trimmedString); // Preventing ultra big strings
		}
	}

	public void setMetrics(@NotNull Map<String, Number> metrics) {
		this.metrics.putAll(metrics);
	}



	private String getTrimmedString(String string) {
		int stringLength = Math.min(string.length(), MAX_CHARS);
		return string.substring(0, stringLength);
	}
	public enum EventType {
		START,
		END_SUCCESS,
		END_ERROR,
		INFO,
		SPOT
	}
}

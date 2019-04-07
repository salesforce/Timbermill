package com.datorama.timbermill.unit;

import org.elasticsearch.action.update.UpdateRequest;

import javax.validation.constraints.NotNull;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

public abstract class Event{

	private static final String DELIMETER = "##@##";
    protected String taskId;
    protected ZonedDateTime time;

    String primaryId;

    protected String name;
    private String parentId;
    private Map<String, String> strings;
    private Map<String, String> texts;
    private Map<String, String> context;
    private Map<String, Number> metrics;

    private List<String> logs;

    private List<String> parentsPath;
	Event(String taskId, String name, @NotNull LogParams logParams, String parentId) {
		if (taskId == null) {
			taskId = generateTaskId(name);
		}
		this.taskId = taskId;
		this.parentId = parentId;
		this.time = ZonedDateTime.now();
		this.name = name;
		this.strings = logParams.getStrings();
		this.texts = logParams.getTexts();
		this.context = logParams.getContext();
		this.metrics = logParams.getMetrics();
		this.logs = logParams.getLogs();
	}

	public static String generateTaskId(String name) {
		String uuid = UUID.randomUUID().toString();
		uuid = uuid.replace("-", "_");
		return name + DELIMETER + uuid;
	}

	private static final int MAX_CHARS = 1000000;

    public String getTaskId() {
        return taskId;
    }

    public String getName() {
        return name;
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

    public ZonedDateTime getTime() {
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

	public Map<String, String> getContext() {
		return context;
	}

    public List<String> getLogs() {
        return logs;
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

	public void setContext(@NotNull Map<String, String> context) {
		for (Entry<String, String> entry : context.entrySet()){
			String trimmedString = getTrimmedString(entry.getValue());
			this.context.put(entry.getKey(), trimmedString); // Preventing ultra big strings
		}
	}

	private String getTrimmedString(String string) {
		int stringLength = Math.min(string.length(), MAX_CHARS);
		return string.substring(0, stringLength);
	}

	public abstract UpdateRequest getUpdateRequest(String index);

	public  boolean isStartEvent(){
		return false;
	}

	public List<String> getParentsPath() {
		return parentsPath;
	}

	public void setParentsPath(List<String> parentsPath) {
		this.parentsPath = parentsPath;
	}

	public String getNameFromId() {
		String[] split = taskId.split(DELIMETER);
		if (name == null){
			if (split.length == 0){
				return null;
			}
			else {
				return split[0];
			}
		}
		else{
			return name;
		}
	}
}

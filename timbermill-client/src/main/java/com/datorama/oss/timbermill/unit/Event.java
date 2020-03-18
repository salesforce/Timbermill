package com.datorama.oss.timbermill.unit;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.validation.constraints.NotNull;

import com.datorama.oss.timbermill.common.ZonedDateTimeJacksonDeserializer;
import com.datorama.oss.timbermill.common.ZonedDateTimeJacksonSerializer;
import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonSubTypes({
		@JsonSubTypes.Type(value = StartEvent.class, name = "StartEvent"),
		@JsonSubTypes.Type(value = InfoEvent.class, name = "InfoEvent"),
		@JsonSubTypes.Type(value = SuccessEvent.class, name = "SuccessEvent"),
		@JsonSubTypes.Type(value = ErrorEvent.class, name = "ErrorEvent"),
		@JsonSubTypes.Type(value = SpotEvent.class, name = "SpotEvent")
}
)
public abstract class Event{

	public static final String EVENT_ID_DELIMITER = "___";
	private static final String OLD_EVENT_ID_DELIMITER = "_";
	private static final String TIMBERMILL_SUFFEIX = "_timbermill2";

	protected String taskId;

	@JsonDeserialize(using = ZonedDateTimeJacksonDeserializer.class)
	@JsonSerialize(using = ZonedDateTimeJacksonSerializer.class)
	protected ZonedDateTime time;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	String primaryId;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	protected String name;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String parentId;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private Map<String, String> strings;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private Map<String, String> text;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private Map<String, String> context;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private Map<String, Number> metrics;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private List<String> logs;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private List<String> parentsPath;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String env;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private Boolean orphan;

	@JsonDeserialize(using = ZonedDateTimeJacksonDeserializer.class)
	@JsonSerialize(using = ZonedDateTimeJacksonSerializer.class)
	@JsonInclude(JsonInclude.Include.NON_NULL)
	protected ZonedDateTime dateToDelete;

	public Event() {
	}

    Event(String taskId, String name, @NotNull LogParams logParams, String parentId) {
		if (taskId == null) {
			taskId = generateTaskId(name);
		}
		this.taskId = taskId;
		this.parentId = parentId;
		this.time = ZonedDateTime.now();
		this.name = name;
		this.strings = logParams.getStrings();
		this.text = logParams.getTexts();
		this.context = logParams.getContext();
		this.metrics = logParams.getMetrics();
		this.logs = logParams.getLogs();
	}

    public String getTaskId() {
        return taskId;
    }

	void setTaskId(String taskId) {
		this.taskId = taskId;
	}

    public String getName() {
        return name;
    }

	public void setName(String name) {
		this.name = name;
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

	public Map<String, String> getStrings() {
        return strings;
    }

	public void setStrings(Map<String, String> strings) {
		this.strings = strings;
	}

	public Map<String, Number> getMetrics() {
		return metrics;
	}

	public void setMetrics(Map<String, Number> metrics) {
		this.metrics = metrics;
	}

	public Map<String, String> getText() {
		return text;
	}

	public void setText(Map<String, String> text) {
		this.text = text;
	}

	public Map<String, String> getContext() {
		return context;
	}

	public void setContext(Map<String, String> context) {
		this.context = context;
	}

    public List<String> getLogs() {
        return logs;
    }

	public void setLogs(List<String> logs) {
		this.logs = logs;
	}

	public List<String> getParentsPath() {
		return parentsPath;
	}

	public void setParentsPath(List<String> parentsPath) {
		this.parentsPath = parentsPath;
	}

	public ZonedDateTime getTime() {
		return time;
	}

	public void setTime(ZonedDateTime time) {
		this.time = time;
	}

	@JsonIgnore
	public ZonedDateTime getEndTime() {
		return null;
	}

    @Override
    public String toString() {
        return "Event{" +
                "id='" + taskId + '\'' +
                '}';
    }

    @JsonIgnore
	public abstract TaskStatus getStatusFromExistingStatus(TaskStatus status);

	@JsonIgnore
    public boolean isStartEvent(){
		return false;
	}

	@JsonIgnore
	public String getNameFromId() {
		if (name == null){
			String taskIdToUse = taskId;
			if (taskIdToUse.endsWith(TIMBERMILL_SUFFEIX)){
				taskIdToUse = taskIdToUse.substring(0, taskIdToUse.length() - TIMBERMILL_SUFFEIX.length());
			}
			String[] split = taskIdToUse.split(EVENT_ID_DELIMITER);
			if (split.length == 1){
				split = taskIdToUse.split(OLD_EVENT_ID_DELIMITER);
				String[] newSplit = Arrays.copyOf(split, split.length - 2);
				return String.join(OLD_EVENT_ID_DELIMITER, newSplit);
			}
			return split[0];
		}
		else{
			return name;
		}
	}

	@JsonIgnore
	public static String generateTaskId(String name) {
		String uuid = UUID.randomUUID().toString();
		uuid = uuid.replace("-", "_");
		return name + EVENT_ID_DELIMITER + uuid;
	}

	public String getEnv() {
		return env;
	}

	public void setEnv(String env) {
		this.env = env;
	}

	public Boolean isOrphan() {
		return orphan;
	}

    public void setOrphan(Boolean orphan) {
		this.orphan = orphan;
	}

	public void setDateToDelete(ZonedDateTime dateToDelete) {
		this.dateToDelete = dateToDelete;
	}

	ZonedDateTime getDateToDelete(long daysRotation) {
		return null;
	}

	@JsonIgnore
	public long estimatedSize() {
		int primaryIdLength = primaryId == null ? 0 : primaryId.length() +15; // "primaryId":"",
		int taskIdLength = taskId == null ? 0 : taskId.length() + 12; // "taskId":"",
		int nameLength = name == null ? 0 : name.length() + 10; // "name":"",
		int parentIdLength = parentId == null ? 0 : parentId.length() + 14; // "parentId":"",
		int envLength = env == null ? 0 : env.length() + 9; // "env":"",

		int stringsSize = strings == null ? 0 : getStringMapSize(strings) + 13; // "strings":{},
		int textsSize = text == null ? 0 : getStringMapSize(text) + 10; // "text":{},
		int contextSize = context == null ? 0 : getStringMapSize(context) + 13; // "context":{},
		int metricsSize = metrics == null ? 0 : getNumberMapSize(metrics)+ 13; // "metrics":{},
		int logsSize = logs == null ? 0 : stringListSize(logs) + 10; // "logs":[],
		int parentsPathSize = parentsPath == null ? 0 : stringListSize(parentsPath) + 14; // "parentPath":[],
		int orphanSize = orphan == null ? 0 : 16; // "orphan":"true",
		int dateToDeleteSize = dateToDelete == null ? 0 : 42; // "orphan":"true",
		return 24 + // {"@type":"StartEvent", },
				34 + // "time":"2020-02-03T16:40:03.898Z",
				primaryIdLength + taskIdLength + nameLength + parentIdLength + envLength + stringsSize + textsSize + contextSize + metricsSize + logsSize + parentsPathSize + orphanSize + dateToDeleteSize;
	}

	@JsonIgnore
	private int stringListSize(List<String> strings) {
		int size = 0;
		for (String string : strings) {
			size += string.length() + 3; //"",
		}
		return size;
	}

	@JsonIgnore
	private int getStringMapSize(Map<String, String> map) {
		int size = 0;
		for (Map.Entry<String, String> entry : map.entrySet()) {
			size += entry.getKey().length() + entry.getValue().length() + 6; // "":"",
		}
		return size;
	}

	@JsonIgnore
	private int getNumberMapSize(Map<String, Number> map) {
		int size = 0;
		for (Map.Entry<String, Number> entry : map.entrySet()) {
			size += entry.getKey().length() + 4; // "":,
		}
		return size;
	}
}

package com.datorama.timbermill;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class Task {
	public enum TaskStatus {
		UNTERMINATED,
		SUCCESS,
		ERROR,
		APP_ERROR,
		CORRUPTED;
	}
	private String taskId;

	private String taskType;

	private String env;

	private TaskStatus status;
	private String parentTaskId;
	private String primaryTaskId;

	private boolean primary;

	@JsonSerialize(using = CustomDateSerializer.class)
	@JsonDeserialize(using = CustomDateDeserializer.class)
	private DateTime startTime;
	@JsonSerialize(using = CustomDateSerializer.class)
	@JsonDeserialize(using = CustomDateDeserializer.class)
	private DateTime endTime;

	private Long totalDuration;

	private Long selfDuration;

	private Long childrenDelta;
	private List<String> origins;
	private String primaryOrigin;
	private Map<String, Object> attributes = new HashMap<>();
	private Map<String, Number> metrics = new HashMap<>();
	private Map<String, String> data = new HashMap<>();

	public Task(String taskId) {
		this.taskId = taskId;
	}
	public Task() {
	}

	public void update(Collection<Event> events) {
		events.stream().forEach(e -> update(e));
	}

	public void update(Event e) {

		Map<String, Object> eAttributes = e.getAttributes();
		Map<String, String> eData = e.getData();
		Map<String, Number> eMetrics = e.getMetrics();
		eAttributes = replaceDotsInKeysAttributes(eAttributes);
		eData = replaceDotsInKeysData(eData);
		eMetrics = replaceDotsInKeysMetrics(eMetrics);

		attributes.putAll(eAttributes);
		metrics.putAll(eMetrics);
		data.putAll(eData);

		switch (e.getEventType()) {
			case START:
				taskType = e.getTaskType();
				parentTaskId = e.getParentTaskId();
				primaryTaskId = e.getPrimaryTaskId();
				primary = (e.getPrimaryTaskId() != null) && e.getPrimaryTaskId().equals(e.getTaskId());
				startTime = e.getTime();
				if (status == null) {
					status = TaskStatus.UNTERMINATED;
				}
				break;
			case INFO:
				if(status == TaskStatus.CORRUPTED){
					updateTimes(e);
				}
				else if(isStartTimeMissing()){
					status = TaskStatus.CORRUPTED;
					taskType = Constants.LOG_WITHOUT_CONTEXT;
					startTime = e.getTime();
					endTime = e.getTime();
				}
				break;
			case SPOT:
				status = TaskStatus.SUCCESS;
				taskType = e.getTaskType();
				parentTaskId = e.getParentTaskId();
				primaryTaskId = e.getPrimaryTaskId();
				primary = (e.getPrimaryTaskId() != null) && e.getPrimaryTaskId().equals(e.getTaskId());
				startTime = e.getTime();
				endTime = e.getTime();
				break;
			case END_ERROR:
				updateEndEvent(e, TaskStatus.ERROR);
				break;
			case END_APP_ERROR:
				updateEndEvent(e, TaskStatus.APP_ERROR);
				break;
			case END_SUCCESS:
				updateEndEvent(e, TaskStatus.SUCCESS);
				break;
		}

	}

	private Map<String, Object> replaceDotsInKeysAttributes(Map<String, Object> oldMap) {
		Map<String, Object> newMap = new HashMap<>();
		for (Entry<String, Object> entry : oldMap.entrySet()){
			newMap.put(entry.getKey().replace(".", "_"), entry.getValue());
		}
		return newMap;
	}

	private Map<String, String> replaceDotsInKeysData(Map<String, String> oldMap) {
		Map<String, String> newMap = new HashMap<>();
		for (Entry<String, String> entry : oldMap.entrySet()){
			newMap.put(entry.getKey().replace(".", "_"), entry.getValue());
		}
		return newMap;
	}

	private Map<String, Number> replaceDotsInKeysMetrics(Map<String, Number> oldMap) {
		Map<String, Number> newMap = new HashMap<>();
		for (Entry<String, Number> entry : oldMap.entrySet()){
			newMap.put(entry.getKey().replace(".", "_"), entry.getValue());
		}
		return newMap;
	}

	private void updateEndEvent(Event e, TaskStatus status) {
		if(this.status == TaskStatus.CORRUPTED){
			updateTimes(e);
		}
		else if(isStartTimeMissing()){
			this.status = TaskStatus.CORRUPTED;
			taskType = Constants.END_WITHOUT_START;
			endTime = e.getTime();
		}
		else{
			this.status = status;
			endTime = e.getTime();
		}
	}

	private void updateTimes(Event e) {
		if (e.getTime().getMillis() < startTime.getMillis() ){
			startTime = e.getTime();
		}
		if (e.getTime().getMillis() > endTime.getMillis() ){
			endTime = e.getTime();
		}
	}


	private boolean isStartTimeMissing() {
		return startTime == null;

	}

	public String getTaskType() {
		return taskType;
	}

	public void setTaskType(String taskType) {
		this.taskType = taskType;
	}

	public TaskStatus getStatus() {
		return status;
	}

	public void setStatus(TaskStatus status) {
		this.status = status;
	}


	public String getTaskId() {
		return taskId;
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

	public boolean isPrimary() {
		return primary;
	}

	public void setPrimary(boolean primary) {
		this.primary = primary;
	}

	public DateTime getStartTime() {
		return startTime;
	}

	public void setStartTime(DateTime startTime) {
		this.startTime = startTime;
	}

	public DateTime getEndTime() {
		return endTime;
	}

	public void setEndTime(DateTime endTime) {
		this.endTime = endTime;
	}

	public long getTotalDuration() {
		return (totalDuration != null) ? totalDuration : 0;
	}

	public void setTotalDuration(long totalDuration) {
		this.totalDuration = totalDuration;
	}

	public long getSelfDuration() {
		return (selfDuration != null) ? selfDuration : 0;
	}

	public void setSelfDuration(long selfDuration) {
		this.selfDuration = selfDuration;
	}

	public Map<String, Object> getAttributes() {
		return attributes;
	}

	public void setAttributes(Map<String, Object> attributes) {
		this.attributes = attributes;
	}

	public Map<String, Number> getMetrics() {
		return metrics;
	}

	public void setMetrics(Map<String, Number> metrics) {
		this.metrics = metrics;
	}

	public Map<String, String> getData() {
		return data;
	}

	public void setData(Map<String, String> data) {
		this.data = data;
	}

	public List<String> getOrigins() {
		return origins;
	}

	public void setOrigins(List<String> origins) {
		this.origins = origins;
	}

	public void setPrimaryOrigin(String primaryOrigin) {
		this.primaryOrigin = primaryOrigin;
	}

	public void setEnv(String env) {
		this.env = env;
	}

	public String getEnv() {
		return env;
	}

	public String getPrimaryOrigin() {
		return primaryOrigin;
	}

	public long getChildrenDelta() {
		return (childrenDelta != null) ? childrenDelta : 0;
	}

	public void updateChildrenDelta(long childrenDelta) {
		this.childrenDelta = (this.childrenDelta != null) ? (this.childrenDelta + childrenDelta) : childrenDelta;
	}

	public boolean taskWithNoStartEvent() {
		return startTime == null;
	}

	@Override
	public int hashCode() {
		int prime = 31;
		int result = 1;
		result = (prime * result) + ((taskId == null) ? 0 : taskId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		Task other = (Task) obj;
		if (taskId == null) {
			if (other.taskId != null) {
				return false;
			}
		} else if (!taskId.equals(other.taskId)) {
			return false;
		}
		return true;
	}

	public static class CustomDateSerializer extends JsonSerializer<DateTime> {

		@Override
		public void serialize(DateTime t, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
			jsonGenerator.writeString(t.toString());
		}

	}

	public static class CustomDateDeserializer extends JsonDeserializer<DateTime> {

		@Override
		public DateTime deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
			return DateTime.parse(jsonParser.getText());
		}

	}

}

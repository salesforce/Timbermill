package com.datorama.timbermill.unit;

import com.datorama.timbermill.common.Constants;
import org.joda.time.DateTime;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Task {
	private String name;
	private TaskStatus status;
	private String parentId;
	private boolean primary;
	private String primaryId;
	private List<String> parentsPath;

	private TaskMetaData meta = new TaskMetaData();

	private Map<String, String> global = new HashMap<>();
	private Map<String, String> string = new HashMap<>();
	private Map<String, String> text = new HashMap<>();
	private Map<String, Number> metric = new HashMap<>();

	public void update(Collection<Event> events) {
		events.forEach(e -> update(e));
	}

	public void update(Event e) {

		Map<String, String> eStrings = e.getStrings();
		Map<String, String> eTexts = e.getTexts();
		Map<String, String> eGlobals = e.getGlobals();
		Map<String, Number> eMetrics = e.getMetrics();

		global.putAll(eGlobals);
		string.putAll(eStrings);
		text.putAll(eTexts);
		metric.putAll(eMetrics);

		switch (e.getEventType()) {
			case START:
				name = e.getName();
				parentId = e.getParentId();
				primaryId = e.getPrimaryId();
				primary = (e.getPrimaryId() != null) && e.getPrimaryId().equals(e.getTaskId());
				meta.setTaskBegin(e.getTime());
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
					name = Constants.LOG_WITHOUT_CONTEXT;
					meta.setTaskBegin(e.getTime());
					meta.setTaskEnd(e.getTime());
				}
				break;
			case SPOT:
				status = TaskStatus.SUCCESS;
				name = e.getName();
				parentId = e.getParentId();
				primaryId = e.getPrimaryId();
				primary = (e.getPrimaryId() != null) && e.getPrimaryId().equals(e.getTaskId());
				meta.setTaskBegin(e.getTime());
				meta.setTaskEnd(e.getTime());
				break;
			case END_ERROR:
				updateEndEvent(e, TaskStatus.ERROR);
				break;
			case END_SUCCESS:
				updateEndEvent(e, TaskStatus.SUCCESS);
				break;
		}
	}

	private void updateEndEvent(Event e, TaskStatus status) {
		if(this.status == TaskStatus.CORRUPTED){
			updateTimes(e);
		}
		else if(isStartTimeMissing()){
			this.status = TaskStatus.CORRUPTED;
			name = Constants.END_WITHOUT_START;
			meta.setTaskEnd(e.getTime());
		}
		else{
			this.status = status;
			meta.setTaskEnd(e.getTime());
		}
	}

	private void updateTimes(Event e) {
		if (e.getTime().getMillis() < meta.getTaskBegin().getMillis() ){
			meta.setTaskBegin(e.getTime());
		}
		if (e.getTime().getMillis() > meta.getTaskEnd().getMillis() ){
			meta.setTaskEnd(e.getTime());
		}
	}


	public boolean isStartTimeMissing() {
		return meta.getTaskBegin() == null;

	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public TaskStatus getStatus() {
		return status;
	}

	public void setStatus(TaskStatus status) {
		this.status = status;
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

	public boolean isPrimary() {
		return primary;
	}

	public void setPrimary(boolean primary) {
		this.primary = primary;
	}

	public DateTime getStartTime() {
		return meta.getTaskBegin();
	}

	public void setStartTime(DateTime startTime) {
		meta.setTaskBegin(startTime);
	}

	public DateTime getEndTime() {
		return meta.getTaskEnd();
	}

	public void setEndTime(DateTime endTime) {
		meta.setTaskEnd(endTime);
	}

	public long getTotalDuration() {
		return meta.getTotalDuration();
	}

	public void setTotalDuration(long totalDuration) {
		meta.setTotalDuration(totalDuration);
	}

	public Map<String, String> getString() {
		return string;
	}

	public void setString(Map<String, String> string) {
		this.string = string;
	}

	public Map<String, Number> getMetric() {
		return metric;
	}

	public void setMetric(Map<String, Number> metric) {
		this.metric = metric;
	}

	public Map<String, String> getText() {
		return text;
	}

	public void setText(Map<String, String> text) {
		this.text = text;
	}

	public Map<String, String> getGlobal() {
		return global;
	}

	public void setGlobal(Map<String, String> global) {
		this.global = global;
	}

	public List<String> getParentsPath() {
		return parentsPath;
	}

	public void setParentsPath(List<String> parentsPath) {
		this.parentsPath = parentsPath;
	}

	public void setEnv(String env) {
		meta.setEnv(env);
	}

	public String getEnv() {
		return meta.getEnv();
	}

    public void setMeta(TaskMetaData meta) {
        this.meta = meta;
    }

	public enum TaskStatus {
		UNTERMINATED,
		SUCCESS,
		ERROR,
		CORRUPTED
	}
}

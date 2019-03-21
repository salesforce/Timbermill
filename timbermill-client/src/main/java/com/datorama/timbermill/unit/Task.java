package com.datorama.timbermill.unit;

import org.joda.time.DateTime;

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

	private Map<String, String> ctx = new HashMap<>();
	private Map<String, String> string = new HashMap<>();
	private Map<String, String> text = new HashMap<>();
	private Map<String, Number> metric = new HashMap<>();

	public Task() {
	}

	public Task(Event e, DateTime startTime, DateTime endTime, TaskStatus status) {
		name = e.getName();
		parentId = e.getParentId();
		primaryId = e.getPrimaryId();
		primary = (e.getPrimaryId() != null) && e.getPrimaryId().equals(e.getTaskId());

		ctx.putAll(e.getContext());
		string.putAll(e.getStrings());
		text.putAll(e.getTexts());
		metric.putAll(e.getMetrics());
		meta.setTaskBegin(startTime);
		meta.setTaskEnd(endTime);
		this.status = status;
		this.parentsPath = e.getParentsPath();
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

	public Map<String, String> getCtx() {
		return ctx;
	}

	public void setCtx(Map<String, String> ctx) {
		this.ctx = ctx;
	}

	public List<String> getParentsPath() {
		return parentsPath;
	}

	public void setParentsPath(List<String> parentsPath) {
		this.parentsPath = parentsPath;
	}

	public void setMeta(TaskMetaData meta) {
        this.meta = meta;
    }

	public enum TaskStatus {
		UNTERMINATED,
		SUCCESS,
		ERROR,
		CORRUPTED //TODO check
	}
}

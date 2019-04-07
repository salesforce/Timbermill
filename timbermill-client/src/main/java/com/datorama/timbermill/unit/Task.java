package com.datorama.timbermill.unit;

import org.apache.commons.lang3.StringUtils;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Task {
	private String name;
	private TaskStatus status;
	private String parentId;
	private Boolean primary;
	private String primaryId;
	private List<String> parentsPath;

	private TaskMetaData meta = new TaskMetaData();

	private Map<String, String> ctx = new HashMap<>();
	private Map<String, String> string = new HashMap<>();
	private Map<String, String> text = new HashMap<>();
	private Map<String, Number> metric = new HashMap<>();
	private String log;

	public Task() {
	}

	public Task(Event e, ZonedDateTime startTime, ZonedDateTime endTime, TaskStatus status) {
		name = e.getNameFromId();
		parentId = e.getParentId();
		primaryId = e.getPrimaryId() == null ? e.getTaskId() : e.getPrimaryId();
		primary = e.getPrimaryId() == null || e.getPrimaryId().equals(e.getTaskId());

		ctx.putAll(e.getContext());
		string.putAll(e.getStrings());
		text.putAll(e.getTexts());
		metric.putAll(e.getMetrics());
		log = StringUtils.join(e.getLogs(), '\n');
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

	public void setPrimary(Boolean primary) {
		this.primary = primary;
	}

	public ZonedDateTime getStartTime() {
		return meta.getTaskBegin();
	}

	public void setStartTime(ZonedDateTime startTime) {
		meta.setTaskBegin(startTime);
	}

	public ZonedDateTime getEndTime() {
		return meta.getTaskEnd();
	}

	public void setEndTime(ZonedDateTime endTime) {
		meta.setTaskEnd(endTime);
	}

	public long getDuration() {
		return meta.getDuration();
	}

	public void setDuration(Long duration) {
		meta.setDuration(duration);
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

	public String getLog() {
		return log;
	}

	public void setLog(String log) {
		this.log = log;
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
		CORRUPTED_SUCCESS, CORRUPTED_ERROR, CORRUPTED
	}
}

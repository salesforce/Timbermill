package com.datorama.timbermill.unit;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.datorama.timbermill.common.Constants.GSON;
import static com.datorama.timbermill.common.Constants.TYPE;

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

	public Task(List<Event> groupedEvents) {
		for (Event e : groupedEvents) {
			String name = e.getNameFromId();
			String parentId = e.getParentId();
			String primaryId = e.getPrimaryId();
			ZonedDateTime startTime = e.getStartTime();
			ZonedDateTime endTime = e.getEndTime();
			List<String> parentsPath = e.getParentsPath();

			if (this.name == null){
				this.name = name;
			}
			else if (name != null && !this.name.equals(name)){
				throw new RuntimeException("Timbermill events with same id must have same name" + this.name + " !=" + name);
			}

			if (this.parentId == null){
				this.parentId = parentId;
			}
			else if (parentId != null && !this.parentId.equals(parentId)){
				throw new RuntimeException("Timbermill events with same id must have same parentId" + this.parentId + " !=" + parentId);
			}

			if (this.primaryId == null){
				this.primaryId = primaryId;
			}
			else if (primaryId != null && !this.primaryId.equals(primaryId)){
				throw new RuntimeException("Timbermill events with same id must have same primaryId" + this.primaryId + " !=" + primaryId);
			}

			if (getStartTime() == null){
				setStartTime(startTime);
			}

			if (getEndTime() == null){
				setEndTime(endTime);
			}

			status = e.getStatus(this.status);

			if (this.parentsPath == null){
				this.parentsPath = parentsPath;
			}
			else if (this.parentsPath.equals(parentsPath)){
				throw new RuntimeException("Timbermill events with same id must have same parentsPath" + this.parentsPath + " !=" + parentsPath);
			}

			ctx.putAll(e.getContext());
			string.putAll(e.getStrings());
			text.putAll(e.getTexts());
			metric.putAll(e.getMetrics());

			if (!e.getLogs().isEmpty()) {
				if (log != null) {
					log += '\n' + StringUtils.join(e.getLogs(), '\n');
				} else {
					log = StringUtils.join(e.getLogs(), '\n');
				}
			}
		}
		ZonedDateTime startTime = getStartTime();
		ZonedDateTime endTime = getEndTime();
		if (isComplete()){
			setDuration(endTime.toInstant().toEpochMilli() - startTime.toInstant().toEpochMilli());
		}


		if (isNotCorrupted()) {
			primary = parentId == null;
		}

	}

	private boolean isNotCorrupted() {
		return status == TaskStatus.UNTERMINATED || isComplete();
	}

	private boolean isComplete() {
		return status == TaskStatus.SUCCESS || status == TaskStatus.ERROR;
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

	public Boolean isPrimary() {
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

	public Long getDuration() {
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

	public UpdateRequest getUpdateRequest(String index, String taskId) {
		UpdateRequest updateRequest = new UpdateRequest(index, TYPE, taskId);
		if (ctx.isEmpty()){
			ctx = null;
		}
		if (string.isEmpty()){
			string = null;
		}
		if (text.isEmpty()){
			text = null;
		}
		if (metric.isEmpty()){
			metric = null;
		}
		if (StringUtils.isEmpty(log)){
			log = null;
		}

		updateRequest.upsert(GSON.toJson(this), XContentType.JSON);

		Map<String, Object> params = new HashMap<>();
		if (getStartTime() != null) {
			params.put("taskBegin", getStartTime().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
			params.put("taskBeginMillis", getStartTime().toInstant().toEpochMilli());
		}
		if (getEndTime() != null) {
			params.put("taskEnd", getEndTime().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
			params.put("taskEndMillis", getEndTime().toInstant().toEpochMilli());
		}
		params.put("name", name);
		params.put("id", taskId);
		params.put("parentId", parentId);
		params.put("primaryId", primaryId);
		params.put("primary", primary);
		params.put("contx", ctx);
		params.put("string", string);
		params.put("text", text);
		params.put("metric", metric);
		params.put("logi", log);
		params.put("parentsPath", parentsPath);
		params.put("status", status);

		String scriptStr = "        if (ctx._source.status.equals( \"SUCCESS\" ) || ctx._source.status.equals( \"ERROR\" )){\n" +
				"            if(params.status.equals( \"SUCCESS\" ) || params.status.equals( \"ERROR\" )){\n" +
				"                throw new Exception(\"Already closed task \"+  params.id);\n" +
				"            }\n" +
				"            else if (params.status.equals( \"UNTERMINATED\")){\n" +
				"                throw new Exception(\"Already started task \"+  params.id);\n" +
				"            }\n" +
				"            else if (params.status.equals( \"CORRUPTED_SUCCESS\")){\n" +
				"                throw new Exception(\"Already ended task \"+  params.id);\n" +
				"            }\n" +
				"            else if (params.status.equals( \"CORRUPTED_ERROR\")){\n" +
				"                throw new Exception(\"Already ended task \"+  params.id);\n" +
				"            }\n" +
				"        }\n" +
				"        else if (ctx._source.status.equals( \"UNTERMINATED\")){\n" +
				"            if(params.status.equals( \"SUCCESS\" ) || params.status.equals( \"ERROR\" )){\n" +
				"                throw new Exception(\"Already started task \"+  params.id);\n" +
				"            }\n" +
				"            else if (params.status.equals( \"UNTERMINATED\")){\n" +
				"                throw new Exception(\"Already started task \"+  params.id);\n" +
				"            }\n" +
				"            else if (params.status.equals( \"CORRUPTED_SUCCESS\")){\n" +
				"                long taskBegin = ZonedDateTime.parse(ctx._source.meta.taskBegin, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant().toEpochMilli();\n" +
				"                ctx._source.meta.duration = params.taskEndMillis - taskBegin;\n" +
				"                ctx._source.meta.taskEnd = params.taskEnd;\n" +
				"                ctx._source.status =  \"SUCCESS\" ;\n" +
				"            }\n" +
				"            else if (params.status.equals( \"CORRUPTED_ERROR\")){\n" +
				"                long taskBegin = ZonedDateTime.parse(ctx._source.meta.taskBegin, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant().toEpochMilli();\n" +
				"                ctx._source.meta.duration = params.taskEndMillis - taskBegin;\n" +
				"                ctx._source.meta.taskEnd = params.taskEnd;\n" +
				"                ctx._source.status = \"ERROR\";\n" +
				"            }\n" +
				"        }\n" +
				"        else if (ctx._source.status.equals( \"CORRUPTED_SUCCESS\")){\n" +
				"            if(params.status.equals( \"SUCCESS\" ) || params.status.equals( \"ERROR\" )){\n" +
				"                throw new Exception(\"Already ended task \"+  params.id);\n" +
				"            }\n" +
				"            else if (params.status.equals( \"UNTERMINATED\")){\n" +
				"                long taskEnd = ZonedDateTime.parse(ctx._source.meta.taskEnd, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant().toEpochMilli();\n" +
				"                ctx._source.meta.duration = taskEnd - params.taskBeginMillis;\n" +
				"                ctx._source.meta.taskBegin = params.taskBegin;\n" +
				"                ctx._source.status =  \"SUCCESS\" ;\n" +
				"            }\n" +
				"            else if (params.status.equals( \"CORRUPTED_SUCCESS\")){\n" +
				"                throw new Exception(\"Already ended task \"+  params.id);\n" +
				"            }\n" +
				"            else if (params.status.equals( \"CORRUPTED_ERROR\")){\n" +
				"                throw new Exception(\"Already ended task \"+  params.id);\n" +
				"            }\n" +
				"        }\n" +
				"        else if (ctx._source.status.equals( \"CORRUPTED_ERROR\")){\n" +
				"            if(params.status.equals( \"SUCCESS\" ) || params.status.equals( \"ERROR\" )){\n" +
				"                throw new Exception(\"Already ended task \"+  params.id);\n" +
				"            }\n" +
				"            else if (params.status.equals( \"UNTERMINATED\")){\n" +
				"                long taskEnd = ZonedDateTime.parse(ctx._source.meta.taskEnd, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant().toEpochMilli();\n" +
				"                ctx._source.meta.duration = taskEnd - params.taskBeginMillis;\n" +
				"                ctx._source.meta.taskBegin = params.taskBegin;\n" +
				"                ctx._source.status =  \"ERROR\" ;\n" +
				"            }\n" +
				"            else if (params.status.equals( \"CORRUPTED_SUCCESS\")){\n" +
				"                throw new Exception(\"Already ended task \"+  params.id);\n" +
				"            }\n" +
				"            else if (params.status.equals( \"CORRUPTED_ERROR\")){\n" +
				"                throw new Exception(\"Already ended task \"+  params.id);\n" +
				"            }\n" +
				"        }\n" +
				"        else {\n" +
				"            if(params.status.equals( \"SUCCESS\" ) || params.status.equals( \"ERROR\" )){\n" +
				"                ctx._source.meta.duration = params.taskEndMillis - params.taskBeginMillis;\n" +
				"                ctx._source.meta.taskEnd = params.taskEnd;\n" +
				"                ctx._source.meta.taskBegin = params.taskBegin;\n" +
				"                ctx._source.status = params.status;\n" +
				"            }\n" +
				"            else if (params.status.equals( \"UNTERMINATED\")){\n" +
				"                ctx._source.meta.taskBegin = params.taskBegin;\n" +
				"                ctx._source.status = params.status;\n" +
				"            }\n" +
				"            else if (params.status.equals( \"CORRUPTED_SUCCESS\")){\n" +
				"                ctx._source.meta.taskEnd = params.taskEnd;\n" +
				"                ctx._source.status = params.status;\n" +
				"            }\n" +
				"            else if (params.status.equals( \"CORRUPTED_ERROR\")){\n" +
				"                ctx._source.meta.taskEnd = params.taskEnd;\n" +
				"                ctx._source.status = params.status;\n" +
				"            }\n" +
				"        }\n" +
				"\n" +
				"        if (params.contx != null) {\n" +
				"            if (ctx._source.ctx == null) {\n" +
				"                ctx._source.ctx = params.contx;\n" +
				"            }\n" +
				"            else {\n" +
				"                ctx._source.ctx.putAll(params.contx);\n" +
				"            }\n" +
				"        }\n" +
				"        if (params.string != null) {\n" +
				"            if (ctx._source.string == null) {\n" +
				"                ctx._source.string = params.string;\n" +
				"            }\n" +
				"            else {\n" +
				"                ctx._source.string.putAll(params.string);\n" +
				"            }\n" +
				"        }\n" +
				"        if (params.text != null) {\n" +
				"            if (ctx._source.text == null) {\n" +
				"                ctx._source.text = params.text;\n" +
				"            }\n" +
				"            else {\n" +
				"                ctx._source.text.putAll(params.text);\n" +
				"            }\n" +
				"        }\n" +
				"        if (params.metric != null) {\n" +
				"            if (ctx._source.metric == null) {\n" +
				"                ctx._source.metric = params.metric;\n" +
				"            }\n" +
				"            else {\n" +
				"                ctx._source.metric.putAll(params.metric);\n" +
				"            }\n" +
				"        }\n" +
				"        if (params.logi != null) {\n" +
				"            if (ctx._source.log == null) {\n" +
				"                ctx._source.log = params.logi;\n" +
				"            } else {\n" +
				"                ctx._source.log += '\n' + params.logi;\n" +
				"            }\n" +
				"        }\n" +
				"        if (params.name != null) {\n" +
				"            ctx._source.name = params.name;\n" +
				"        }\n" +
				"        if (params.parentId != null) {\n" +
				"            ctx._source.parentId = params.parentId;\n" +
				"        }\n" +
				"        if (params.primaryId != null) {\n" +
				"            ctx._source.primaryId = params.primaryId;\n" +
				"        }\n" +
				"        if (params.primary != null && ctx._source.primary == null) {\n" +
				"            ctx._source.primary = params.primary;\n" +
				"        }\n" +
				"        if (params.parentsPath != null) {\n" +
				"            ctx._source.parentsPath = params.parentsPath;\n" +
				"        }";
		Script script = new Script(ScriptType.INLINE, "painless", scriptStr, params);
		updateRequest.script(script);
		return updateRequest;
	}

	public enum TaskStatus {
		UNTERMINATED,
		SUCCESS,
		ERROR,
		CORRUPTED_SUCCESS,
		CORRUPTED_ERROR,
		CORRUPTED
	}
}

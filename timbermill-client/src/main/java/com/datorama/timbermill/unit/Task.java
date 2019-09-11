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
import static com.datorama.timbermill.unit.Task.TaskStatus.*;

public class Task {

	public static final String ALREADY_STARTED = "ALREADY_STARTED";
	public static final String ALREADY_CLOSED = "ALREADY_CLOSED";
	public static final String CORRUPTED_REASON = "corruptedReason";
	private String env;

	private String name;
	private TaskStatus status;
	private String parentId;
	private String primaryId;
	private List<String> parentsPath;

	private TaskMetaData meta = new TaskMetaData();

	private Map<String, String> ctx = new HashMap<>();
	private Map<String, String> string = new HashMap<>();
	private Map<String, String> text = new HashMap<>();
	private Map<String, Number> metric = new HashMap<>();
	private String log;
	private Boolean orphan;

	public Task() {
	}

	public Task(List<Event> groupedEvents, String env) {
		this.env = env;
		for (Event e : groupedEvents) {
			String name = e.getNameFromId();
			String parentId = e.getParentId();
			String primaryId = e.getPrimaryId();
			ZonedDateTime startTime = e.getTime();
			ZonedDateTime endTime = e.getEndTime();
			List<String> parentsPath = e.getParentsPath();

			if (this.name == null){
				this.name = name;
			}
			else if (name != null && !this.name.equals(name)){
				throw new RuntimeException("Timbermill events with same id must have same name " + this.name + " !=" + name);
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

			status = e.getStatusFromExistingStatus(this.status);

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
			if (e.isOrphan() != null) {
				orphan = e.isOrphan();
			}
		}
		ZonedDateTime startTime = getStartTime();
		ZonedDateTime endTime = getEndTime();
		if (isComplete()){
			setDuration(endTime.toInstant().toEpochMilli() - startTime.toInstant().toEpochMilli());
		}

	}

	private boolean isComplete() {
		return status == SUCCESS || status == ERROR;
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

	public UpdateRequest getUpdateRequest(String index, String taskId) {
		UpdateRequest updateRequest = new UpdateRequest(index, TYPE, taskId);
		if (string != null && string.isEmpty()){
			string = null;
		}
		if (text != null && text.isEmpty()){
			text = null;
		}
		if (metric != null && metric.isEmpty()){
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
		params.put("parentId", parentId);
		params.put("primaryId", primaryId);
		params.put("contx", ctx);
		params.put("string", string);
		params.put("text", text);
		params.put("metric", metric);
		params.put("logi", log);
		params.put("parentsPath", parentsPath);
		params.put("status", status);
		if (orphan != null){
			params.put("orphan", orphan);
		}

		String scriptStr =
				"        if (params.orphan != null && !params.orphan) {" +
				"            ctx._source.orphan = false;" +
				"        }" +
				"        else if (params.status.equals( \"" + CORRUPTED + "\")){" +
				"                ctx._source.status =  \"" + CORRUPTED + "\" ;" +
				"        }" +
				"        else if (ctx._source.status.equals( \"" + SUCCESS + "\" ) || ctx._source.status.equals( \"" + ERROR + "\" )){" +
				"            if(params.status.equals( \"" + SUCCESS + "\" ) || params.status.equals( \"" + ERROR + "\" )){" +
				"                ctx._source.status =  \"" + CORRUPTED + "\" ;" +
				"                ctx._source.ctx.put(\"" + CORRUPTED_REASON + "\",\"" + ALREADY_CLOSED + "\");" +
				"            }" +
				"            else if (params.status.equals( \"" + UNTERMINATED + "\")){" +
				"                ctx._source.status =  \"" + CORRUPTED + "\" ;" +
				"                ctx._source.ctx.put(\"" + CORRUPTED_REASON + "\",\"" + ALREADY_STARTED + "\");" +
				"            }" +
				"            else if (params.status.equals( \"" + PARTIAL_SUCCESS + "\")){" +
				"                ctx._source.status =  \"" + CORRUPTED + "\" ;" +
				"                ctx._source.ctx.put(\"" + CORRUPTED_REASON + "\",\"" + ALREADY_CLOSED + "\");" +
				"            }" +
				"            else if (params.status.equals( \"" + PARTIAL_ERROR + "\")){" +
				"                ctx._source.status =  \"" + CORRUPTED + "\" ;" +
				"                ctx._source.ctx.put(\"" + CORRUPTED_REASON + "\",\"" + ALREADY_CLOSED + "\");" +
				"            }" +
				"        }" +
				"        else if (ctx._source.status.equals( \"" + UNTERMINATED + "\")){" +
				"            if(params.status.equals( \"" + SUCCESS + "\" ) || params.status.equals( \"" + ERROR + "\" )){" +
				"                ctx._source.status =  \"" + CORRUPTED + "\" ;" +
				"                ctx._source.ctx.put(\"" + CORRUPTED_REASON + "\",\"" + ALREADY_STARTED + "\");" +
				"            }" +
				"            else if (params.status.equals( \"" + UNTERMINATED + "\")){" +
				"                ctx._source.status =  \"" + CORRUPTED + "\" ;" +
				"                ctx._source.ctx.put(\"" + CORRUPTED_REASON + "\",\"" + ALREADY_STARTED + "\");" +
				"            }" +
				"            else if (params.status.equals( \"" + PARTIAL_SUCCESS + "\")){" +
				"                long taskBegin = ZonedDateTime.parse(ctx._source.meta.taskBegin, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant().toEpochMilli();" +
				"                ctx._source.meta.duration = params.taskEndMillis - taskBegin;" +
				"                ctx._source.meta.taskEnd = params.taskEnd;" +
				"                ctx._source.status =  \"" + SUCCESS + "\" ;" +
				"            }" +
				"            else if (params.status.equals( \"" + PARTIAL_ERROR + "\")){" +
				"                long taskBegin = ZonedDateTime.parse(ctx._source.meta.taskBegin, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant().toEpochMilli();" +
				"                ctx._source.meta.duration = params.taskEndMillis - taskBegin;" +
				"                ctx._source.meta.taskEnd = params.taskEnd;" +
				"                ctx._source.status = \"" + ERROR + "\";" +
				"            }" +
				"        }" +
				"        else if (ctx._source.status.equals( \"" + PARTIAL_SUCCESS + "\")){" +
				"            if(params.status.equals( \"" + SUCCESS + "\" ) || params.status.equals( \"" + ERROR + "\" )){" +
				"                ctx._source.status =  \"" + CORRUPTED + "\" ;" +
				"                ctx._source.ctx.put(\"" + CORRUPTED_REASON + "\",\"" + ALREADY_CLOSED + "\");" +
				"            }" +
				"            if (params.status.equals( \"" + UNTERMINATED + "\")){" +
				"                long taskEnd = ZonedDateTime.parse(ctx._source.meta.taskEnd, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant().toEpochMilli();" +
				"                ctx._source.meta.duration = taskEnd - params.taskBeginMillis;" +
				"                ctx._source.meta.taskBegin = params.taskBegin;" +
				"                ctx._source.status =  \"" + SUCCESS + "\" ;" +
				"            }" +
				"            else if (params.status.equals( \"" + PARTIAL_SUCCESS + "\")){" +
				"                ctx._source.status =  \"" + CORRUPTED + "\" ;" +
				"                ctx._source.ctx.put(\"" + CORRUPTED_REASON + "\",\"" + ALREADY_CLOSED + "\");" +
				"            }" +
				"            else if (params.status.equals( \"" + PARTIAL_ERROR + "\")){" +
				"                ctx._source.status =  \"" + CORRUPTED + "\" ;" +
				"                ctx._source.ctx.put(\"" + CORRUPTED_REASON + "\",\"" + ALREADY_CLOSED + "\");" +
				"            }" +
				"        }" +
				"        else if (ctx._source.status.equals( \"" + PARTIAL_ERROR + "\")){" +
				"            if(params.status.equals( \"" + SUCCESS + "\" ) || params.status.equals( \"" + ERROR + "\" )){" +
				"                ctx._source.status =  \"" + CORRUPTED + "\" ;" +
				"                ctx._source.ctx.put(\"" + CORRUPTED_REASON + "\",\"" + ALREADY_CLOSED + "\");" +
				"            }" +
				"            else if (params.status.equals( \"" + UNTERMINATED + "\")){" +
				"                long taskEnd = ZonedDateTime.parse(ctx._source.meta.taskEnd, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant().toEpochMilli();" +
				"                ctx._source.meta.duration = taskEnd - params.taskBeginMillis;" +
				"                ctx._source.meta.taskBegin = params.taskBegin;" +
				"                ctx._source.status =  \"" + ERROR + "\" ;" +
				"            }" +
				"            else if (params.status.equals( \"" + PARTIAL_SUCCESS + "\")){" +
				"                ctx._source.status =  \"" + CORRUPTED + "\" ;" +
				"                ctx._source.ctx.put(\"" + CORRUPTED_REASON + "\",\"" + ALREADY_CLOSED + "\");" +
				"            }" +
				"            else if (params.status.equals( \"" + PARTIAL_ERROR + "\")){" +
				"                ctx._source.status =  \"" + CORRUPTED + "\" ;" +
				"                ctx._source.ctx.put(\"" + CORRUPTED_REASON + "\",\"" + ALREADY_CLOSED + "\");" +
				"            }" +
				"            else if (params.status.equals( \"" + CORRUPTED + "\")){" +
				"                ctx._source.status =  \"" + CORRUPTED + "\" ;" +
				"            }" +
				"        }" +
				"        else if (ctx._source.status.equals( \"" + PARTIAL_INFO_ONLY + "\")){" +
				"            if(params.status.equals( \"" + SUCCESS + "\" ) || params.status.equals( \"" + ERROR + "\" )){" +
				"                ctx._source.meta.duration = params.taskEndMillis - params.taskBeginMillis;" +
				"                ctx._source.meta.taskEnd = params.taskEnd;" +
				"                ctx._source.meta.taskBegin = params.taskBegin;" +
				"                ctx._source.status = params.status;" +
				"            }" +
				"            else if (params.status.equals( \"" + UNTERMINATED + "\")){" +
				"                ctx._source.meta.taskBegin = params.taskBegin;" +
				"                ctx._source.status = params.status;" +
				"            }" +
				"            else if (params.status.equals( \"" + PARTIAL_SUCCESS + "\")){" +
				"                ctx._source.meta.taskEnd = params.taskEnd;" +
				"                ctx._source.status = params.status;" +
				"            }" +
				"            else if (params.status.equals( \"" + PARTIAL_ERROR + "\")){" +
				"                ctx._source.meta.taskEnd = params.taskEnd;" +
				"                ctx._source.status = params.status;" +
				"            }" +
				"        }" +
				"        else {" + //Corrupted
				"            ctx._source.status =  \"" + CORRUPTED + "\" ;" +
				"        }" +
				"        if (params.contx != null) {" +
				"            if (ctx._source.ctx == null) {" +
				"                ctx._source.ctx = params.contx;" +
				"            }" +
				"            else {" +
				"                ctx._source.ctx.putAll(params.contx);" +
				"            }" +
				"        }" +
				"        if (params.string != null) {" +
				"            if (ctx._source.string == null) {" +
				"                ctx._source.string = params.string;" +
				"            }" +
				"            else {" +
				"                ctx._source.string.putAll(params.string);" +
				"            }" +
				"        }" +
				"        if (params.text != null) {" +
				"            if (ctx._source.text == null) {" +
				"                ctx._source.text = params.text;" +
				"            }" +
				"            else {" +
				"                ctx._source.text.putAll(params.text);" +
				"            }" +
				"        }" +
				"        if (params.metric != null) {" +
				"            if (ctx._source.metric == null) {" +
				"                ctx._source.metric = params.metric;" +
				"            }" +
				"            else {" +
				"                ctx._source.metric.putAll(params.metric);" +
				"            }" +
				"        }" +
				"        if (params.logi != null) {" +
				"            if (ctx._source.log == null) {" +
				"                ctx._source.log = params.logi;" +
				"            } else {" +
				"                ctx._source.log += '\n' + params.logi;" +
				"            }" +
				"        }" +
				"        if (params.name != null) {" +
				"            ctx._source.name = params.name;" +
				"        }" +
				"        if (params.parentId != null) {" +
				"            ctx._source.parentId = params.parentId;" +
				"        }" +
				"        if (params.primaryId != null) {" +
				"            ctx._source.primaryId = params.primaryId;" +
				"        }" +
				"        if (params.primary != null && ctx._source.primary == null) {" +
				"            ctx._source.primary = params.primary;" +
				"        }" +
				"        if (params.parentsPath != null) {" +
				"            ctx._source.parentsPath = params.parentsPath;" +
				"        }" +
				"        if (params.orphan != null && params.orphan) {" +
				"            ctx._source.orphan = true;" +
				"        }";
		Script script = new Script(ScriptType.INLINE, "painless", scriptStr, params);
		updateRequest.script(script);
		return updateRequest;
	}

	public enum TaskStatus {
		UNTERMINATED,
		SUCCESS,
		ERROR,
		PARTIAL_SUCCESS,
		PARTIAL_ERROR,
		PARTIAL_INFO_ONLY,
		CORRUPTED
	}
}

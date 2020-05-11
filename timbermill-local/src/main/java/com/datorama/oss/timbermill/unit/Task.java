package com.datorama.oss.timbermill.unit;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.oss.timbermill.common.Constants;
import com.datorama.oss.timbermill.common.ElasticsearchUtil;

import static com.datorama.oss.timbermill.common.Constants.CORRUPTED_REASON;
import static com.datorama.oss.timbermill.unit.TaskStatus.CORRUPTED;

public class Task {

	private static final Logger LOG = LoggerFactory.getLogger(Task.class);
	private static final String OLD_EVENT_ID_DELIMITER = "_";
	private static final String TIMBERMILL_SUFFIX = "_timbermill2";

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

	public Task(List<Event> events, long daysRotation) {
		Map<String, List<Event>> collect = events.stream().collect(Collectors.groupingBy(e -> e.isAdoptedEvent() ? "ADOPTED" : "REGULAR"));

		if (collect.containsKey("REGULAR")) {
			for (Event e : collect.get("REGULAR")) {
				String env = e.getEnv();
				if (this.env == null || this.env.equals(env)) {
					this.env = env;
				} else {
					throw new RuntimeException("Timbermill events with same id must have same env " + this.env + " !=" + env);
				}
				String name = e.getName();
				if (name == null) {
					name = getNameFromId(name, e.getTaskId());
				}

				String parentId = e.getParentId();

				ZonedDateTime startTime = e.getTime();
				ZonedDateTime endTime = e.getEndTime();

				if (this.name == null) {
					this.name = name;
				}

				if (this.parentId == null) {
					this.parentId = parentId;
				} else if (parentId != null && !this.parentId.equals(parentId)) {
					LOG.warn("Found different parentId for same task. Flagged task [{}] as corrupted. parentId 1 [{}], parentId 2 [{}]", e.getTaskId(), this.parentId, parentId);
					status = CORRUPTED;
					string.put(CORRUPTED_REASON, "Different parentIds");
				}

				if (getStartTime() == null) {
					setStartTime(startTime);
				}

				if (getEndTime() == null) {
					setEndTime(endTime);
				}

				ZonedDateTime dateToDelete = e.getDateToDelete(daysRotation);
				if (dateToDelete != null) {
					this.setDateToDelete(dateToDelete);
				}

				status = e.getStatusFromExistingStatus(this.status);

				if (e.getStrings() != null && !e.getStrings().isEmpty()) {
					string.putAll(e.getStrings());
				}
				if (e.getText() != null && !e.getText().isEmpty()) {
					text.putAll(e.getText());
				}
				if (e.getMetrics() != null && !e.getMetrics().isEmpty()) {
					metric.putAll(e.getMetrics());
				}

				if (e.getLogs() != null && !e.getLogs().isEmpty()) {
					if (log != null) {
						log += '\n' + StringUtils.join(e.getLogs(), '\n');
					} else {
						log = StringUtils.join(e.getLogs(), '\n');
					}
				}

				String primaryId = e.getPrimaryId();
				if (this.primaryId == null) {
					this.primaryId = primaryId;
				} else if (primaryId != null && !this.primaryId.equals(primaryId)) {
					if (this.primaryId.equals(e.getTaskId())) {
						this.primaryId = primaryId; // Override with actual primary id
					} else if (!primaryId.equals(e.getTaskId())) {
						List<String> evenTypeList = events.stream().map(ev -> ev.getClass().getSimpleName()).collect(Collectors.toList());
						LOG.warn(evenTypeList.toString());
						LOG.warn("Found different primaryId for same task. Flagged task [{}] as corrupted. primaryId 1 [{}], primaryId 2 [{}]", e.getTaskId(), this.primaryId, primaryId);
						status = CORRUPTED;
						string.put(CORRUPTED_REASON, "Different primaryIds");
					}
				}
				List<String> parentsPath = e.getParentsPath();
				if (this.parentsPath == null) {
					this.parentsPath = parentsPath;
				} else {
					if (parentsPath != null && !parentsPath.equals(this.parentsPath)) {
						List<String> evenTypeList = events.stream().map(ev -> ev.getClass().getSimpleName()).collect(Collectors.toList());
						LOG.warn(evenTypeList.toString());
						LOG.warn("Found different parentsPath for same task. Flagged task [{}] as corrupted. parentsPath 1 [{}], parentsPath 2 [{}]", e.getTaskId(), this.parentsPath, parentsPath);
						status = CORRUPTED;
						string.put(CORRUPTED_REASON, "Different parentsPaths");
					}
				}
				if (e.getContext() != null && !e.getContext().isEmpty()) {
					ctx.putAll(e.getContext());
				}
				if (e.isOrphan() != null) {
					if (orphan == null) {
						orphan = e.isOrphan();
					} else {
						orphan = orphan || e.isOrphan();
					}
				}
			}
		}
		ZonedDateTime startTime = getStartTime();
		ZonedDateTime endTime = getEndTime();
		if (isComplete()){
			long duration = ElasticsearchUtil.getTimesDuration(startTime, endTime);
			setDuration(duration);
		}

		if (collect.containsKey("ADOPTED")) {
			List<Event> adoptedEvents = collect.get("ADOPTED");
			if (adoptedEvents.size() > 1) {
				LOG.warn("More than 1 adopted events. Events {}", adoptedEvents);
			}
			for (Event adoptedEvent : adoptedEvents) {
				String primaryId = adoptedEvent.getPrimaryId();
				if (primaryId == null) {
					LOG.warn("No primary ID for adopted event. Adopted {} \n Task {}", adoptedEvent.toString(), this.toString());
				} else {
					this.primaryId = primaryId;
				}

				List<String> parentsPath = adoptedEvent.getParentsPath();
				if (parentsPath == null || parentsPath.isEmpty()) {
					LOG.warn("Empty parent path for adopted event. Adopted {} \n Task {}", adoptedEvent.toString(), this.toString());
				} else {
					this.parentsPath = parentsPath;
				}

				Map<String, String> adoptedContext = adoptedEvent.getContext();
				if (adoptedContext != null && !adoptedContext.isEmpty()) {
					for (Map.Entry<String, String> entry : adoptedContext.entrySet()) {
						this.ctx.putIfAbsent(entry.getKey(), entry.getValue());
					}
				}
				orphan = false;
			}
		}
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

	public ZonedDateTime getDateToDelete() {
		return meta.getDateToDelete();
	}

	public void setDateToDelete(ZonedDateTime dateToDelete) {
		meta.setDateToDelete(dateToDelete);
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
		UpdateRequest updateRequest = new UpdateRequest(index, Constants.TYPE, taskId);
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

		updateRequest.upsert(Constants.GSON.toJson(this), XContentType.JSON);

		Map<String, Object> params = new HashMap<>();
		if (getStartTime() != null) {
			params.put("taskBegin", getStartTime().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
			params.put("taskBeginMillis", getStartTime().toInstant().toEpochMilli());
		}
		if (getEndTime() != null) {
			params.put("taskEnd", getEndTime().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
			params.put("taskEndMillis", getEndTime().toInstant().toEpochMilli());
		}
		if (getDateToDelete() != null) {
			params.put("dateToDelete", getDateToDelete().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
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
		params.put("status", status != null ? status.toString() : "");
		if (orphan != null){
			params.put("orphan", orphan);
		}

		Script script = new Script(ScriptType.STORED, null, Constants.TIMBERMILL_SCRIPT, params);
		updateRequest.script(script);
		return updateRequest;
	}

	@Override public String toString() {
		return "Task{" +
				"env='" + env + '\'' +
				", name='" + name + '\'' +
				", status=" + status +
				", parentId='" + parentId + '\'' +
				", primaryId='" + primaryId + '\'' +
				", parentsPath=" + parentsPath +
				", meta={" + meta + "}" +
				", ctx=" + ctx +
				", string=" + string +
				", text=" + text +
				", metric=" + metric +
				", log='" + log + '\'' +
				", orphan=" + orphan +
				'}';
	}

	public static String getNameFromId(String name, String taskId) {
		try {
			if (name == null) {
				String taskIdToUse = taskId;
				if (taskIdToUse.endsWith(TIMBERMILL_SUFFIX)) {
					taskIdToUse = taskIdToUse.substring(0, taskIdToUse.length() - TIMBERMILL_SUFFIX.length());
				}
				String[] split = taskIdToUse.split(Event.EVENT_ID_DELIMITER);
				if (split.length == 1) {
					split = taskIdToUse.split(OLD_EVENT_ID_DELIMITER);
					String[] newSplit = Arrays.copyOf(split, split.length - 2);
					return String.join(OLD_EVENT_ID_DELIMITER, newSplit);
				}
				return split[0];
			} else {
				return name;
			}
		} catch (Exception e){
			LOG.warn("Couldn't get name from ID {}", taskId);
			return taskId;
		}
	}
}

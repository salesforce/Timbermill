package com.datorama.timbermill.unit;

import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import javax.validation.constraints.NotNull;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import static com.datorama.timbermill.common.Constants.GSON;
import static com.datorama.timbermill.common.Constants.TYPE;

public class StartEvent extends Event {
    public StartEvent(String taskId, String name, @NotNull LogParams logParams, String primaryId, String parentId) {
        super(taskId, name, logParams, parentId);
        if (primaryId == null){
            this.primaryId = this.taskId;
        } else {
            this.primaryId = primaryId;
        }
    }

    public StartEvent(String name, @NotNull LogParams logParams, String primaryId, String parentId) {
        this(null, name, logParams, primaryId, parentId);
    }

    @Override
    public UpdateRequest getUpdateRequest(String index) {
        UpdateRequest updateRequest = new UpdateRequest(index, TYPE, taskId);
        Task task = new Task(this, this.time, null, Task.TaskStatus.UNTERMINATED);

        Map<String, String> ctx = task.getCtx();
        if (ctx.isEmpty()){
            task.setCtx(null);
        }
        Map<String, String> string = task.getString();
        if (string.isEmpty()){
            task.setString(null);
        }
        Map<String, String> text = task.getText();
        if (text.isEmpty()){
            task.setText(null);
        }
        Map<String, Number> metric = task.getMetric();
        if (metric.isEmpty()){
            task.setMetric(null);
        }
        String log = task.getLog();
        if (log.isEmpty()){
            task.setLog(null);
        }

        updateRequest.upsert(GSON.toJson(task), XContentType.JSON);

        Map<String, Object> params = new HashMap<>();
        params.put("taskBegin", time.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        params.put("taskBeginMillis", time.toInstant().toEpochMilli());
        params.put("name", task.getName());
        params.put("parentId", task.getParentId());
        params.put("primaryId", task.getPrimaryId());
        params.put("primary", task.isPrimary());
        params.put("contx", ctx);
        params.put("string", string);
        params.put("text", text);
        params.put("metric", metric);
        params.put("logi", log);
        params.put("parentsPath", task.getParentsPath());

        String scriptStr = "if (ctx._source.meta.taskEnd != null) {" +
                "long taskEnd = ZonedDateTime.parse(ctx._source.meta.taskEnd, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant().toEpochMilli(); " +
                "ctx._source.meta.duration = taskEnd - params.taskBeginMillis;}" +
                "if (ctx._source.status.equals(\"" + Task.TaskStatus.CORRUPTED_SUCCESS + "\")) {ctx._source.status = \"" + Task.TaskStatus.SUCCESS + "\";}" +
                "else if (ctx._source.status.equals(\"" + Task.TaskStatus.CORRUPTED_ERROR + "\")) {ctx._source.status = \"" + Task.TaskStatus.ERROR + "\";}" +
                "else {ctx._source.status = \"" + Task.TaskStatus.UNTERMINATED + "\";}" +
                "ctx._source.meta.taskBegin = params.taskBegin;" +
                "ctx._source.name = params.name;" +
                "ctx._source.parentId = params.parentId;" +
                "ctx._source.primaryId = params.primaryId;" +
                "ctx._source.primary = params.primary;" +
                "if (ctx._source.ctx == null) {ctx._source.ctx = params.contx;} else {ctx._source.ctx.putAll(params.contx);}" +
                "if (ctx._source.string == null) {ctx._source.string = params.string;} else {ctx._source.string.putAll(params.string);}" +
                "if (ctx._source.text == null) {ctx._source.text = params.text;} else {ctx._source.text.putAll(params.text);}" +
                "if (ctx._source.metric == null) {ctx._source.metric = params.metric;} else {ctx._source.metric.putAll(params.metric);}" +
                "if (!params.logi.isEmpty()){ if (ctx._source.log == null) {ctx._source.log = params.logi;} else {ctx._source.log += \"\n\" + params.logi;}}" +
                "ctx._source.parentsPath = params.parentsPath;";

        Script script = new Script(ScriptType.INLINE, "painless", scriptStr, params);
        updateRequest.script(script);
        return updateRequest;
    }

    @Override
    public boolean isStartEvent() {
        return true;
    }
}

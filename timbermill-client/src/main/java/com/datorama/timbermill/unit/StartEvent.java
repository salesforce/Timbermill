package com.datorama.timbermill.unit;

import com.google.gson.Gson;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import javax.validation.constraints.NotNull;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

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
    public UpdateRequest getUpdateRequest(String index, Gson gson) {
        UpdateRequest updateRequest = new UpdateRequest(index, TYPE, taskId);
        Task task = new Task(this, this.time, null, Task.TaskStatus.UNTERMINATED);
        updateRequest.upsert(gson.toJson(task), XContentType.JSON);

        Map<String, Object> params = new HashMap<>();
        params.put("taskBegin", time.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        params.put("taskBeginMillis", time.toInstant().toEpochMilli());
        params.put("name", task.getName());
        params.put("parentId", task.getParentId());
        params.put("primaryId", task.getPrimaryId());
        params.put("primary", task.isPrimary());
        params.put("contx", task.getCtx());
        params.put("string", task.getString());
        params.put("text", task.getText());
        params.put("metric", task.getMetric());
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
                "ctx._source.ctx.putAll(params.contx);" +
                "ctx._source.string.putAll(params.string);" +
                "ctx._source.text.putAll(params.text);" +
                "ctx._source.metric.putAll(params.metric);" +
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

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

public class SuccessEvent extends Event {
    public SuccessEvent(String taskId, @NotNull LogParams logParams) {
        super(taskId, null, logParams, null);
    }

    public UpdateRequest getUpdateRequest(String index) {
        UpdateRequest updateRequest = new UpdateRequest(index, TYPE, taskId);
        Task task = new Task(this, null, time, Task.TaskStatus.CORRUPTED_SUCCESS);
        updateRequest.upsert(GSON.toJson(task), XContentType.JSON);

        Map<String, Object> params = new HashMap<>();
        params.put("taskEnd", time.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        params.put("taskEndMillis", time.toInstant().toEpochMilli());
        params.put("contx", task.getCtx());
        params.put("string", task.getString());
        params.put("text", task.getText());
        params.put("metric", task.getMetric());

        String scriptStr = "if (ctx._source.meta.taskBegin != null) {" +
                "long taskBegin = ZonedDateTime.parse(ctx._source.meta.taskBegin, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant().toEpochMilli(); " +
                "ctx._source.meta.duration = params.taskEndMillis - taskBegin; }" +
                "ctx._source.meta.taskEnd = params.taskEnd;" +
                "if (ctx._source.status.equals(\"" + Task.TaskStatus.UNTERMINATED + "\")) {ctx._source.status = \"" + Task.TaskStatus.SUCCESS + "\";}" +
                "else if (ctx._source.status.equals(\"" + Task.TaskStatus.CORRUPTED + "\")) {ctx._source.status = \"" + Task.TaskStatus.CORRUPTED_SUCCESS + "\";}" +
                "ctx._source.ctx.putAll(params.contx);" +
                "ctx._source.string.putAll(params.string);" +
                "ctx._source.text.putAll(params.text);" +
                "ctx._source.metric.putAll(params.metric);";

        Script script = new Script(ScriptType.INLINE, "painless", scriptStr, params);
        updateRequest.script(script);
        return updateRequest;
    }
}

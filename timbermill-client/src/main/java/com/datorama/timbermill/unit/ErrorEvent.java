package com.datorama.timbermill.unit;

import com.google.gson.Gson;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import javax.validation.constraints.NotNull;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import static com.datorama.timbermill.common.Constants.TYPE;

public class ErrorEvent extends Event {
    public ErrorEvent(String taskId, @NotNull LogParams logParams) {
        super(taskId, null, logParams, null);
    }

    @Override
    public UpdateRequest getUpdateRequest(String index, Gson gson) {
        UpdateRequest updateRequest = new UpdateRequest(index, TYPE, taskId);
        Map<String, Object> params = new HashMap<>();
        params.put("taskEnd", time.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        params.put("taskEndMillis", time.toInstant().toEpochMilli());
        params.put("status", Task.TaskStatus.ERROR);

        String scriptStr = "if (ctx._source.meta.taskBegin != null) {" +
                "long taskBegin = ZonedDateTime.parse(ctx._source.meta.taskBegin, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant().toEpochMilli(); " +
                "ctx._source.meta.duration = params.taskEndMillis - taskBegin; }" +
                "ctx._source.status = params.status;" +
                "ctx._source.meta.taskEnd = params.taskEnd;";

        Script script = new Script(ScriptType.INLINE, "painless", scriptStr, params);
        updateRequest.script(script);
        return updateRequest;
    }
}

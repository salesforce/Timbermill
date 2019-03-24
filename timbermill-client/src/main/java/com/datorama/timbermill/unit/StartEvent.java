package com.datorama.timbermill.unit;

import com.google.gson.Gson;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;

import static com.datorama.timbermill.common.Constants.TYPE;

public class StartEvent extends Event {
    public StartEvent(String name, @NotNull LogParams logParams, String primaryId, String parentId) {
        super(null, name, logParams, parentId);
        if (primaryId == null){
            this.primaryId = taskId;
        } else {
            this.primaryId = primaryId;
        }
    }

    @Override
    public UpdateRequest getUpdateRequest(String index, Gson gson) {
        UpdateRequest updateRequest = new UpdateRequest(index, TYPE, taskId);
        Task task = new Task(this, this.time, null, Task.TaskStatus.UNTERMINATED);
        updateRequest.upsert(gson.toJson(task), XContentType.JSON);

        Map<String, Object> params = new HashMap<>();
        params.put("taskBegin", time.toInstant().toEpochMilli());
        params.put("status", Task.TaskStatus.UNTERMINATED);

        String scriptStr = "if (ctx._source.meta.taskEnd != null) {" +
                "long taskEnd = ZonedDateTime.parse(ctx._source.meta.taskEnd, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant().toEpochMilli(); " +
                "ctx._source.meta.duration = taskEnd - params.taskBegin; }" +
                "ctx._source.status = params.status;" +
                "ctx._source.meta.taskBegin = params.taskBegin;";

        Script script = new Script(ScriptType.INLINE, "painless", scriptStr, params);
        updateRequest.script(script);
        return updateRequest;
    }

    @Override
    public boolean isStartEvent() {
        return true;
    }
}

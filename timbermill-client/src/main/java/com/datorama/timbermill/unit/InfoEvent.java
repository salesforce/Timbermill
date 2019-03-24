package com.datorama.timbermill.unit;

import com.google.gson.Gson;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;

import javax.validation.constraints.NotNull;

import static com.datorama.timbermill.common.Constants.TYPE;

public class InfoEvent extends Event {
    public InfoEvent(String taskId, @NotNull LogParams logParams) {
        super(taskId, null, logParams, null);
    }

    @Override
    public UpdateRequest getUpdateRequest(String index, Gson gson) {
        UpdateRequest updateRequest = new UpdateRequest(index, TYPE, taskId);
        Task task = new Task(this, time, time, Task.TaskStatus.CORRUPTED);

        updateRequest.upsert(gson.toJson(task), XContentType.JSON);


        task = new Task(this, null, null, null);
        task.setPrimary(null);
        task.setMeta(null);
        updateRequest.doc(gson.toJson(task), XContentType.JSON);
        return updateRequest;
    }
}

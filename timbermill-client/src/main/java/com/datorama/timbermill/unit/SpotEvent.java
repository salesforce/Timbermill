package com.datorama.timbermill.unit;

import com.google.gson.Gson;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;

import javax.validation.constraints.NotNull;

import static com.datorama.timbermill.common.Constants.TYPE;

public class SpotEvent extends Event {
    public SpotEvent(String name, @NotNull LogParams logParams, String primaryId, String parentId) {
        super(null, name, logParams, parentId);
        if (primaryId == null){
            this.primaryId = taskId;
        } else {
            this.primaryId = primaryId;
        }
    }

    @Override
    public UpdateRequest getUpdateRequest(String index, Gson gson) {
        UpdateRequest updateRequest = new UpdateRequest(index, TYPE, getTaskId());
        Task task = new Task(this, time, time, Task.TaskStatus.SUCCESS);
        updateRequest.upsert(gson.toJson(task), XContentType.JSON);
        updateRequest.doc(gson.toJson(task), XContentType.JSON);
        return updateRequest;
    }

    @Override
    public boolean isStartEvent(){
        return true;
    }
}

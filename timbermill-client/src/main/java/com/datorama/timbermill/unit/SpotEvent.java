package com.datorama.timbermill.unit;

import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;

import javax.validation.constraints.NotNull;

import static com.datorama.timbermill.common.Constants.GSON;
import static com.datorama.timbermill.common.Constants.TYPE;

public class SpotEvent extends Event {
    private final Task.TaskStatus status;

    public SpotEvent(String name, @NotNull LogParams logParams, String primaryId, String parentId, Task.TaskStatus status) {
        super(null, name, logParams, parentId);
        if (primaryId == null){
            this.primaryId = taskId;
        } else {
            this.primaryId = primaryId;
        }
        this.status = status;
    }

    @Override
    public UpdateRequest getUpdateRequest(String index) {
        UpdateRequest updateRequest = new UpdateRequest(index, TYPE, getTaskId());
        Task task = new Task(this, time, time, status);

        if (task.getCtx().isEmpty()){
            task.setCtx(null);
        }
        if (task.getString().isEmpty()){
            task.setString(null);
        }
        if (task.getText().isEmpty()){
            task.setText(null);
        }
        if (task.getMetric().isEmpty()){
            task.setMetric(null);
        }
        if (task.getLog().isEmpty()){
            task.setLog(null);
        }

        updateRequest.upsert(GSON.toJson(task), XContentType.JSON);
        updateRequest.doc(GSON.toJson(task), XContentType.JSON);
        return updateRequest;
    }

    @Override
    public boolean isStartEvent(){
        return true;
    }
}

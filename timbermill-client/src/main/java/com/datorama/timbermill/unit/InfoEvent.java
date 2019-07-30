package com.datorama.timbermill.unit;

import javax.validation.constraints.NotNull;

public class InfoEvent extends Event {
    public InfoEvent(String taskId, @NotNull LogParams logParams) {
        super(taskId, null, logParams, null);
    }

    @Override
    public Task.TaskStatus getStatus(Task.TaskStatus status) {
        if (status == null){
            return Task.TaskStatus.CORRUPTED;
        }
        else {
            return status;
        }
    }
}

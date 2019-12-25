package com.datorama.timbermill.unit;

import com.fasterxml.jackson.annotation.JsonIgnore;

import javax.validation.constraints.NotNull;

public class InfoEvent extends Event {

    public InfoEvent() {
    }

    public InfoEvent(String taskId, @NotNull LogParams logParams) {
        super(taskId, null, logParams, null);
    }

    @JsonIgnore
    @Override
    public Task.TaskStatus getStatusFromExistingStatus(Task.TaskStatus status) {
        return getTaskStatus(status);
    }

    public static Task.TaskStatus getTaskStatus(Task.TaskStatus status) {
        if (status == Task.TaskStatus.UNTERMINATED){
            return Task.TaskStatus.UNTERMINATED;
        }
        else if (status == Task.TaskStatus.PARTIAL_SUCCESS){
            return Task.TaskStatus.PARTIAL_SUCCESS;
        }
        else if (status == Task.TaskStatus.SUCCESS){
            return Task.TaskStatus.SUCCESS;
        }
        else if (status == Task.TaskStatus.PARTIAL_ERROR){
            return Task.TaskStatus.PARTIAL_ERROR;
        }
        else if (status == Task.TaskStatus.ERROR){
            return Task.TaskStatus.ERROR;
        }
        else if (status == Task.TaskStatus.CORRUPTED){
            return Task.TaskStatus.CORRUPTED;
        }
        else {
            return Task.TaskStatus.PARTIAL_INFO_ONLY;
        }
    }
}

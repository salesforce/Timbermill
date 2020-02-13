package com.datorama.oss.timbermill.unit;

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
    public TaskStatus getStatusFromExistingStatus(TaskStatus status) {
        return getTaskStatus(status);
    }

    private static TaskStatus getTaskStatus(TaskStatus status) {
        if (status == TaskStatus.UNTERMINATED){
            return TaskStatus.UNTERMINATED;
        }
        else if (status == TaskStatus.PARTIAL_SUCCESS){
            return TaskStatus.PARTIAL_SUCCESS;
        }
        else if (status == TaskStatus.SUCCESS){
            return TaskStatus.SUCCESS;
        }
        else if (status == TaskStatus.PARTIAL_ERROR){
            return TaskStatus.PARTIAL_ERROR;
        }
        else if (status == TaskStatus.ERROR){
            return TaskStatus.ERROR;
        }
        else if (status == TaskStatus.CORRUPTED){
            return TaskStatus.CORRUPTED;
        }
        else {
            return TaskStatus.PARTIAL_INFO_ONLY;
        }
    }
}

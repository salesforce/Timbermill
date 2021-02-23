package com.datorama.oss.timbermill.unit;

import com.fasterxml.jackson.annotation.JsonIgnore;

import javax.validation.constraints.NotNull;
import java.time.ZonedDateTime;

public class ErrorEvent extends Event {

    private static final long serialVersionUID = Event.serialVersionUID;

    public ErrorEvent() {
    }

    public ErrorEvent(String taskId, @NotNull LogParams logParams) {
        super(taskId, null, logParams, null);
    }

    public ZonedDateTime getEndTime() {
        return time;
    }

    @JsonIgnore
    @Override
    public TaskStatus getStatusFromExistingStatus(TaskStatus taskStatus, ZonedDateTime startTime, ZonedDateTime taskEndTime, String taskParentId, String taskName) {
        if (taskStatus == TaskStatus.UNTERMINATED){
            return TaskStatus.ERROR;
        }
        else if (taskStatus == TaskStatus.PARTIAL_SUCCESS || taskStatus == TaskStatus.SUCCESS){
            return SuccessEvent.handleDifferentCloseStatus(strings);
        }
        else if (taskStatus == TaskStatus.PARTIAL_ERROR || taskStatus == TaskStatus.ERROR){
            return SuccessEvent.handleAlreadyClosed(this.time, taskEndTime, strings, taskStatus);
        }
        else if (taskStatus == TaskStatus.CORRUPTED){
            return TaskStatus.CORRUPTED;
        }
        else {
            return TaskStatus.PARTIAL_ERROR;
        }
    }

}

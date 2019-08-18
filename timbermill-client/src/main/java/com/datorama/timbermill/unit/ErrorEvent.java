package com.datorama.timbermill.unit;

import com.fasterxml.jackson.annotation.JsonIgnore;

import javax.validation.constraints.NotNull;
import java.time.ZonedDateTime;

public class ErrorEvent extends Event {
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
    public Task.TaskStatus getStatusFromExistingStatus(Task.TaskStatus status) {
        if (status == Task.TaskStatus.UNTERMINATED){
            return Task.TaskStatus.ERROR;
        }
        else if (status == Task.TaskStatus.CORRUPTED){
            return Task.TaskStatus.CORRUPTED_ERROR;
        }
        else {
            return Task.TaskStatus.CORRUPTED_ERROR;
        }
    }

    

}

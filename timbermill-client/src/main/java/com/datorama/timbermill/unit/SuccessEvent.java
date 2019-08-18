package com.datorama.timbermill.unit;

import com.fasterxml.jackson.annotation.JsonIgnore;

import javax.validation.constraints.NotNull;
import java.time.ZonedDateTime;

public class SuccessEvent extends Event {
    public SuccessEvent() {
    }

    public SuccessEvent(String taskId, @NotNull LogParams logParams) {
        super(taskId, null, logParams, null);
    }

    @JsonIgnore
    public ZonedDateTime getEndTime() {
        return time;
    }

    @JsonIgnore
    @Override
    public Task.TaskStatus getStatusFromExistingStatus(Task.TaskStatus status) {
        if (status == Task.TaskStatus.UNTERMINATED){
            return Task.TaskStatus.SUCCESS;
        }
        else if (status == Task.TaskStatus.CORRUPTED){
            return Task.TaskStatus.CORRUPTED_SUCCESS;
        }
        else {
            return Task.TaskStatus.CORRUPTED_SUCCESS;
        }
    }

}

package com.datorama.timbermill.unit;

import com.fasterxml.jackson.annotation.JsonIgnore;

import javax.validation.constraints.NotNull;
import java.time.ZonedDateTime;

import static com.datorama.timbermill.unit.Task.*;

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
        else if (status == Task.TaskStatus.PARTIAL_SUCCESS){
            getContext().put(CORRUPTED_REASON, ALREADY_CLOSED);
            return Task.TaskStatus.CORRUPTED;
        }
        else if (status == Task.TaskStatus.SUCCESS){
            getContext().put(CORRUPTED_REASON, ALREADY_CLOSED);
            return Task.TaskStatus.CORRUPTED;
        }
        else if (status == Task.TaskStatus.PARTIAL_ERROR){
            getContext().put(CORRUPTED_REASON, ALREADY_CLOSED);
            return Task.TaskStatus.CORRUPTED;
        }
        else if (status == TaskStatus.ERROR){
            getContext().put(CORRUPTED_REASON, ALREADY_CLOSED);
            return Task.TaskStatus.CORRUPTED;
        }
        else if (status == TaskStatus.CORRUPTED){
            return Task.TaskStatus.CORRUPTED;
        }
        else {
            return TaskStatus.PARTIAL_SUCCESS;
        }
    }

}

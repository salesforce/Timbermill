package com.datorama.timbermill.unit;

import javax.validation.constraints.NotNull;
import java.time.ZonedDateTime;

public class SuccessEvent extends Event {
    public SuccessEvent() {
    }

    public SuccessEvent(String taskId, @NotNull LogParams logParams) {
        super(taskId, null, logParams, null);
    }

    public ZonedDateTime getEndTime() {
        return time;
    }

    @Override
    public Task.TaskStatus getStatus(Task.TaskStatus status) {
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

package com.datorama.timbermill.unit;

import javax.validation.constraints.NotNull;
import java.time.ZonedDateTime;

public class ErrorEvent extends Event {
    public ErrorEvent(String taskId, @NotNull LogParams logParams) {
        super(taskId, null, logParams, null);
    }

    public ZonedDateTime getEndTime() {
        return time;
    }

    @Override
    public Task.TaskStatus getStatus(Task.TaskStatus status) {
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

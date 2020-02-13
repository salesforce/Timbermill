package com.datorama.timbermill.unit;

import com.fasterxml.jackson.annotation.JsonIgnore;

import javax.validation.constraints.NotNull;
import java.time.ZonedDateTime;
import java.util.Map;

import static com.datorama.timbermill.unit.Task.ALREADY_CLOSED;
import static com.datorama.timbermill.unit.Task.CORRUPTED_REASON;

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
        return getTaskStatus(status, getStrings());
    }

    public static Task.TaskStatus getTaskStatus(Task.TaskStatus status, Map<String, String> string) {
        if (status == Task.TaskStatus.UNTERMINATED){
            return Task.TaskStatus.ERROR;
        }
        else if (status == Task.TaskStatus.PARTIAL_SUCCESS){
            string.put(CORRUPTED_REASON, ALREADY_CLOSED);
            return Task.TaskStatus.CORRUPTED;
        }
        else if (status == Task.TaskStatus.SUCCESS){
            string.put(CORRUPTED_REASON, ALREADY_CLOSED);
            return Task.TaskStatus.CORRUPTED;
        }
        else if (status == Task.TaskStatus.PARTIAL_ERROR){
            string.put(CORRUPTED_REASON, ALREADY_CLOSED);
            return Task.TaskStatus.CORRUPTED;
        }
        else if (status == Task.TaskStatus.ERROR){
            string.put(CORRUPTED_REASON, ALREADY_CLOSED);
            return Task.TaskStatus.CORRUPTED;
        }
        else if (status == Task.TaskStatus.CORRUPTED){
            return Task.TaskStatus.CORRUPTED;
        }
        else {
            return Task.TaskStatus.PARTIAL_ERROR;
        }
    }

}

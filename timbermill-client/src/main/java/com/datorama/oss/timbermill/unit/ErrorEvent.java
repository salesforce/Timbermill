package com.datorama.oss.timbermill.unit;

import com.fasterxml.jackson.annotation.JsonIgnore;

import javax.validation.constraints.NotNull;
import java.time.ZonedDateTime;
import java.util.Map;

import static com.datorama.oss.timbermill.common.Constants.ALREADY_CLOSED;
import static com.datorama.oss.timbermill.common.Constants.CORRUPTED_REASON;

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
    public TaskStatus getStatusFromExistingStatus(TaskStatus status) {
        return getTaskStatus(status, getStrings());
    }

    private static TaskStatus getTaskStatus(TaskStatus status, Map<String, String> string) {
        if (status == TaskStatus.UNTERMINATED){
            return TaskStatus.ERROR;
        }
        else if (status == TaskStatus.PARTIAL_SUCCESS){
            string.put(CORRUPTED_REASON, ALREADY_CLOSED);
            return TaskStatus.CORRUPTED;
        }
        else if (status == TaskStatus.SUCCESS){
            string.put(CORRUPTED_REASON, ALREADY_CLOSED);
            return TaskStatus.CORRUPTED;
        }
        else if (status == TaskStatus.PARTIAL_ERROR){
            string.put(CORRUPTED_REASON, ALREADY_CLOSED);
            return TaskStatus.CORRUPTED;
        }
        else if (status == TaskStatus.ERROR){
            string.put(CORRUPTED_REASON, ALREADY_CLOSED);
            return TaskStatus.CORRUPTED;
        }
        else if (status == TaskStatus.CORRUPTED){
            return TaskStatus.CORRUPTED;
        }
        else {
            return TaskStatus.PARTIAL_ERROR;
        }
    }

}

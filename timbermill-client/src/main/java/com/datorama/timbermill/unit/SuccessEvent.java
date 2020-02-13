package com.datorama.timbermill.unit;

import com.fasterxml.jackson.annotation.JsonIgnore;

import javax.validation.constraints.NotNull;
import java.time.ZonedDateTime;
import java.util.Map;

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
        return getTaskStatus(status, getStrings());
    }

    private static TaskStatus getTaskStatus(TaskStatus status, Map<String, String> string) {
        if (status == TaskStatus.UNTERMINATED){
            return TaskStatus.SUCCESS;
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
            return TaskStatus.PARTIAL_SUCCESS;
        }
    }

}

package com.datorama.oss.timbermill.unit;

import com.datorama.oss.timbermill.common.Constants;
import com.fasterxml.jackson.annotation.JsonIgnore;

import javax.validation.constraints.NotNull;
import java.time.ZonedDateTime;
import java.util.Map;

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
    public TaskStatus getStatusFromExistingStatus(TaskStatus status) {
        return getTaskStatus(status, getStrings());
    }

    private static TaskStatus getTaskStatus(TaskStatus status, Map<String, String> string) {
        if (status == TaskStatus.UNTERMINATED){
            return TaskStatus.SUCCESS;
        }
        else if (status == TaskStatus.PARTIAL_SUCCESS){
            string.put(Constants.CORRUPTED_REASON, Constants.ALREADY_CLOSED);
            return TaskStatus.CORRUPTED;
        }
        else if (status == TaskStatus.SUCCESS){
            string.put(Constants.CORRUPTED_REASON, Constants.ALREADY_CLOSED);
            return TaskStatus.CORRUPTED;
        }
        else if (status == TaskStatus.PARTIAL_ERROR){
            string.put(Constants.CORRUPTED_REASON, Constants.ALREADY_CLOSED);
            return TaskStatus.CORRUPTED;
        }
        else if (status == TaskStatus.ERROR){
            string.put(Constants.CORRUPTED_REASON, Constants.ALREADY_CLOSED);
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

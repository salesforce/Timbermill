package com.datorama.oss.timbermill.unit;

import java.time.ZonedDateTime;

import javax.validation.constraints.NotNull;

import com.datorama.oss.timbermill.common.Constants;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Maps;

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
        return getTaskStatus(status);
    }

    private TaskStatus getTaskStatus(TaskStatus status) {
        if (status == TaskStatus.UNTERMINATED){
            return TaskStatus.SUCCESS;
        }
        else if (status == TaskStatus.PARTIAL_SUCCESS){
            if (strings == null){
                strings = Maps.newHashMap();
            }
            strings.put(Constants.CORRUPTED_REASON, Constants.ALREADY_CLOSED);
            return TaskStatus.CORRUPTED;
        }
        else if (status == TaskStatus.SUCCESS){
            if (strings == null){
                strings = Maps.newHashMap();
            }
            strings.put(Constants.CORRUPTED_REASON, Constants.ALREADY_CLOSED);
            return TaskStatus.CORRUPTED;
        }
        else if (status == TaskStatus.PARTIAL_ERROR){
            if (strings == null){
                strings = Maps.newHashMap();
            }
            strings.put(Constants.CORRUPTED_REASON, Constants.ALREADY_CLOSED);
            return TaskStatus.CORRUPTED;
        }
        else if (status == TaskStatus.ERROR){
            if (strings == null){
                strings = Maps.newHashMap();
            }
            strings.put(Constants.CORRUPTED_REASON, Constants.ALREADY_CLOSED);
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

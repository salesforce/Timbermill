package com.datorama.oss.timbermill.unit;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Maps;

import javax.validation.constraints.NotNull;
import java.time.ZonedDateTime;

import static com.datorama.oss.timbermill.common.Constants.*;
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
        if (status == TaskStatus.UNTERMINATED){
            return TaskStatus.ERROR;
        }
        else if (status == TaskStatus.PARTIAL_SUCCESS || status == TaskStatus.SUCCESS || status == TaskStatus.PARTIAL_ERROR || status == TaskStatus.ERROR){
            updateStringsWithReason();
            return TaskStatus.CORRUPTED;
        }
        else if (status == TaskStatus.CORRUPTED){
            return TaskStatus.CORRUPTED;
        }
        else {
            return TaskStatus.PARTIAL_ERROR;
        }
    }

    private void updateStringsWithReason() {
        if (strings == null) {
            strings = Maps.newHashMap();
        }
        strings.put(CORRUPTED_REASON, ALREADY_CLOSED);
    }

}

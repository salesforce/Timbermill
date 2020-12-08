package com.datorama.oss.timbermill.unit;

import com.fasterxml.jackson.annotation.JsonIgnore;

import javax.validation.constraints.NotNull;
import java.time.ZonedDateTime;

public class InfoEvent extends Event {

    private static final long serialVersionUID = Event.serialVersionUID;

    public InfoEvent() {
    }

    public InfoEvent(String taskId, @NotNull LogParams logParams) {
        super(taskId, null, logParams, null);
    }

    @JsonIgnore
    @Override
    public TaskStatus getStatusFromExistingStatus(TaskStatus taskStatus, ZonedDateTime startTime, ZonedDateTime taskEndTime, String taskParentId, String taskName) {
        if (taskStatus == null){
            return TaskStatus.PARTIAL_INFO_ONLY;
        }
        else {
            return taskStatus;
        }
    }
}

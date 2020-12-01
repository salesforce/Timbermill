package com.datorama.oss.timbermill.unit;

import java.time.ZonedDateTime;

import com.datorama.oss.timbermill.common.Constants;
import com.datorama.oss.timbermill.common.TimbermillDatesUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Maps;

import javax.validation.constraints.NotNull;

public class StartEvent extends Event {

    public static final String ALREADY_STARTED_DIFFERENT_PARENT = "ALREADY_STARTED_DIFFERENT_PARENT";
    public static final String ALREADY_STARTED_DIFFERENT_NAME = "ALREADY_STARTED_DIFFERENT_NAME";
    public static final String ALREADY_STARTED_DIFFERENT_START_TIME = "ALREADY_STARTED_DIFFERENT_START_TIME";
    public static final long serialVersionUID = Event.serialVersionUID;

    public StartEvent() {
    }

    public StartEvent(String taskId, String name, @NotNull LogParams logParams, String parentId) {
        super(taskId, name, logParams, parentId);
    }

    @JsonIgnore
    @Override
    public TaskStatus getStatusFromExistingStatus(TaskStatus taskStatus, ZonedDateTime taskStartTime, ZonedDateTime taskEndTime, String taskParentId, String taskName) {
        if (taskStatus == TaskStatus.UNTERMINATED || taskStatus == TaskStatus.SUCCESS || taskStatus == TaskStatus.ERROR){
            return handleAlreadyStarted(taskStartTime, taskName, taskParentId, taskStatus);
        }
        else if (taskStatus == TaskStatus.PARTIAL_SUCCESS){
            return TaskStatus.SUCCESS;
        }
        else if (taskStatus == TaskStatus.PARTIAL_ERROR){
            return TaskStatus.ERROR;
        }
        else if (taskStatus == TaskStatus.CORRUPTED){
            return TaskStatus.CORRUPTED;
        }
        else {
            return TaskStatus.UNTERMINATED;
        }
    }

    private TaskStatus handleAlreadyStarted(ZonedDateTime taskStartTime, String taskName, String taskParentId, TaskStatus taskStatus) {
        if (strings == null){
            strings = Maps.newHashMap();
        }
        if (taskStartTime != null && !taskStartTime.equals(this.time)) {
            strings.put(Constants.CORRUPTED_REASON, ALREADY_STARTED_DIFFERENT_START_TIME);
            return TaskStatus.CORRUPTED;
        } else if (taskName != null && !taskName.equals(this.name)) {
            strings.put(Constants.CORRUPTED_REASON, ALREADY_STARTED_DIFFERENT_NAME);
            return TaskStatus.CORRUPTED;
        } else if (taskParentId != null && !taskParentId.equals(this.parentId)) {
            strings.put(Constants.CORRUPTED_REASON, ALREADY_STARTED_DIFFERENT_PARENT);
            return TaskStatus.CORRUPTED;
        } else {
            return taskStatus;
        }
    }

    @JsonIgnore
    @Override
    public boolean isStartEvent() {
        return true;
    }

    @JsonIgnore
    @Override
    public ZonedDateTime getStartTime() {
        return time;
    }

    @JsonIgnore
    @Override
    ZonedDateTime getDateToDelete(long defaultDaysRotation) {
        return TimbermillDatesUtils.getDateToDeleteWithDefault(defaultDaysRotation, this.dateToDelete);
    }
}

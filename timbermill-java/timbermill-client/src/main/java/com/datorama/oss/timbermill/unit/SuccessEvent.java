package com.datorama.oss.timbermill.unit;

import java.time.ZonedDateTime;
import java.util.Map;

import javax.validation.constraints.NotNull;

import com.datorama.oss.timbermill.common.Constants;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Maps;

public class SuccessEvent extends Event {
    public static final String ALREADY_CLOSED_DIFFERENT_CLOSE_TIME = "ALREADY_CLOSED_DIFFERENT_CLOSE_TIME";
    public static final String ALREADY_CLOSED_DIFFERENT_CLOSE_STATUS = "ALREADY_CLOSED_DIFFERENT_CLOSE_STATUS";
    public static final long serialVersionUID = Event.serialVersionUID;

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
    public TaskStatus getStatusFromExistingStatus(TaskStatus taskStatus, ZonedDateTime taskStartTime, ZonedDateTime taskEndTime, String taskParentId, String taskName) {
        if (taskStatus == TaskStatus.UNTERMINATED){
            return TaskStatus.SUCCESS;
        }
        else if (taskStatus == TaskStatus.PARTIAL_ERROR || taskStatus == TaskStatus.ERROR){
            return handleDifferentCloseStatus(strings);
        }
        else if (taskStatus == TaskStatus.PARTIAL_SUCCESS || taskStatus == TaskStatus.SUCCESS){
            return handleAlreadyClosed(this.time, taskEndTime, strings, taskStatus);
        }
        else if (taskStatus == TaskStatus.CORRUPTED){
            return TaskStatus.CORRUPTED;
        }
        else {
            return TaskStatus.PARTIAL_SUCCESS;
        }
    }

    static TaskStatus handleDifferentCloseStatus(Map<String, String> strings) {
        if (strings == null){
            strings = Maps.newHashMap();
        }
        strings.put(Constants.CORRUPTED_REASON, ALREADY_CLOSED_DIFFERENT_CLOSE_STATUS);
        return TaskStatus.CORRUPTED;
    }

    static TaskStatus handleAlreadyClosed(ZonedDateTime eventTime, ZonedDateTime taskEndTime, Map<String, String> strings, TaskStatus taskStatus) {
        if (taskEndTime != null && !taskEndTime.equals(eventTime)) {
            if (strings == null){
                strings = Maps.newHashMap();
            }
            strings.put(Constants.CORRUPTED_REASON, ALREADY_CLOSED_DIFFERENT_CLOSE_TIME);
            return TaskStatus.CORRUPTED;
        } else {
            return taskStatus;
        }
    }
 }

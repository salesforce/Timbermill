package com.datorama.oss.timbermill.unit;

import java.time.ZonedDateTime;
import java.util.Map;

import com.datorama.oss.timbermill.common.Constants;
import com.datorama.oss.timbermill.common.TimbermillDatesUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;

import javax.validation.constraints.NotNull;

public class StartEvent extends Event {

    public static final String ALREADY_STARTED = "ALREADY_STARTED";

    public StartEvent() {
    }

    public StartEvent(String taskId, String name, @NotNull LogParams logParams, String parentId) {
        super(taskId, name, logParams, parentId);
    }

    @JsonIgnore
    @Override
    public TaskStatus getStatusFromExistingStatus(TaskStatus status) {
        return getTaskStatus(status, getStrings());
    }

    private static TaskStatus getTaskStatus(TaskStatus status, Map<String, String> string) {
        if (status == TaskStatus.UNTERMINATED){
            string.put(Constants.CORRUPTED_REASON, ALREADY_STARTED);
            return TaskStatus.CORRUPTED;
        }
        else if (status == TaskStatus.PARTIAL_SUCCESS){
            return TaskStatus.SUCCESS;
        }
        else if (status == TaskStatus.SUCCESS){
            string.put(Constants.CORRUPTED_REASON, ALREADY_STARTED);
            return TaskStatus.CORRUPTED;
        }
        else if (status == TaskStatus.PARTIAL_ERROR){
            return TaskStatus.ERROR;
        }
        else if (status == TaskStatus.ERROR){
            string.put(Constants.CORRUPTED_REASON, ALREADY_STARTED);
            return TaskStatus.CORRUPTED;
        }
        else if (status == TaskStatus.CORRUPTED){
            return TaskStatus.CORRUPTED;
        }
        else {
            return TaskStatus.UNTERMINATED;
        }
    }

    @JsonIgnore
    @Override
    public boolean isStartEvent() {
        return true;
    }

    @JsonIgnore
    @Override
    ZonedDateTime getDateToDelete(long defaultDaysRotation) {
        return TimbermillDatesUtils.getDateToDeleteWithDefault(defaultDaysRotation, this.dateToDelete);
    }
}

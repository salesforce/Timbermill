package com.datorama.timbermill.unit;

import java.time.ZonedDateTime;
import java.util.Map;

import com.datorama.timbermill.common.TimbermillUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;

import javax.validation.constraints.NotNull;

import static com.datorama.timbermill.unit.Task.*;

public class StartEvent extends Event {

    public StartEvent() {
    }

    public StartEvent(String taskId, String name, @NotNull LogParams logParams, String primaryId, String parentId) {
        super(taskId, name, logParams, parentId);
        if (primaryId == null){
            this.primaryId = this.taskId;
        } else {
            this.primaryId = primaryId;
        }
    }

    //Testing
    public StartEvent(String name, @NotNull LogParams logParams, String primaryId, String parentId) {
        this(null, name, logParams, primaryId, parentId);
    }

    @JsonIgnore
    @Override
    public Task.TaskStatus getStatusFromExistingStatus(Task.TaskStatus status) {
        return getTaskStatus(status, getStrings());
    }

    private static TaskStatus getTaskStatus(TaskStatus status, Map<String, String> string) {
        if (status == TaskStatus.UNTERMINATED){
            string.put(CORRUPTED_REASON, ALREADY_STARTED);
            return TaskStatus.CORRUPTED;
        }
        else if (status == TaskStatus.PARTIAL_SUCCESS){
            return TaskStatus.SUCCESS;
        }
        else if (status == TaskStatus.SUCCESS){
            string.put(CORRUPTED_REASON, ALREADY_STARTED);
            return TaskStatus.CORRUPTED;
        }
        else if (status == TaskStatus.PARTIAL_ERROR){
            return TaskStatus.ERROR;
        }
        else if (status == TaskStatus.ERROR){
            string.put(CORRUPTED_REASON, ALREADY_STARTED);
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
        return TimbermillUtils.getDateToDeleteWithDefault(defaultDaysRotation, this.dateToDelete);
    }
}

package com.datorama.timbermill.unit;

import java.time.ZonedDateTime;

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
        if (status == TaskStatus.UNTERMINATED){
            getContext().put(CORRUPTED_REASON, ALREADY_STARTED);
            return Task.TaskStatus.CORRUPTED;
        }
        else if (status == Task.TaskStatus.PARTIAL_SUCCESS){
            return Task.TaskStatus.SUCCESS;
        }
        else if (status == Task.TaskStatus.SUCCESS){
            getContext().put(CORRUPTED_REASON, ALREADY_STARTED);
            return Task.TaskStatus.CORRUPTED;
        }
        else if (status == Task.TaskStatus.PARTIAL_ERROR){
            return Task.TaskStatus.ERROR;
        }
        else if (status == TaskStatus.ERROR){
            getContext().put(CORRUPTED_REASON, ALREADY_STARTED);
            return Task.TaskStatus.CORRUPTED;
        }
        else if (status == TaskStatus.CORRUPTED){
            return Task.TaskStatus.CORRUPTED;
        }
        else {
            return Task.TaskStatus.UNTERMINATED;
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

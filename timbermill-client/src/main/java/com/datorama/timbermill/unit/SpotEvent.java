package com.datorama.timbermill.unit;

import com.datorama.timbermill.common.TimbermillUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;

import javax.validation.constraints.NotNull;

import java.time.ZonedDateTime;

public class SpotEvent extends Event {
    private Task.TaskStatus status;

    public SpotEvent() {
    }

    public SpotEvent(String taskId, String name, String primaryId, String parentId, Task.TaskStatus status, @NotNull LogParams logParams) {
        super(taskId, name, logParams, parentId);
        if (primaryId == null){
            this.primaryId = this.taskId;
        } else {
            this.primaryId = primaryId;
        }
        this.status = status;
    }

    @JsonIgnore
    public ZonedDateTime getEndTime() {
        return time;
    }

    @JsonIgnore
    @Override
    public Task.TaskStatus getStatusFromExistingStatus(Task.TaskStatus status) {
        return this.status;
    }

    public Task.TaskStatus getStatus() {
        return this.status;
    }

    public void setStatus(Task.TaskStatus status) {
        this.status = status;
    }

    @JsonIgnore
    @Override
    public boolean isStartEvent(){
        return true;
    }

    @JsonIgnore
    @Override
    ZonedDateTime getDateToDelete(long defaultDaysRotation) {
        return TimbermillUtils.getDateToDeleteWithDefault(defaultDaysRotation, this.dateToDelete);
    }
}

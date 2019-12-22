package com.datorama.timbermill.unit;

import com.fasterxml.jackson.annotation.JsonIgnore;

import javax.validation.constraints.NotNull;

import java.time.ZonedDateTime;

public class SpotEvent extends Event {
    private Task.TaskStatus status;

    public SpotEvent() {
    }

    public SpotEvent(String name, @NotNull LogParams logParams, String primaryId, String parentId, Task.TaskStatus status) {
        super(null, name, logParams, parentId);
        if (primaryId == null){
            this.primaryId = taskId;
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
}

package com.datorama.oss.timbermill.unit;

import com.datorama.oss.timbermill.common.TimbermillDatesUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;

import javax.validation.constraints.NotNull;
import java.time.ZonedDateTime;

public class SpotEvent extends Event {

    private static final long serialVersionUID = Event.serialVersionUID;
    private TaskStatus status;

    public SpotEvent() {
    }

    public SpotEvent(String taskId, String name, String parentId, TaskStatus status, @NotNull LogParams logParams) {
        super(taskId, name, logParams, parentId);
        this.status = status;
    }

    @JsonIgnore
    @Override
    public ZonedDateTime getStartTime() {
        return time;
    }

    @JsonIgnore
    @Override
    public ZonedDateTime getEndTime() {
        return time;
    }

    @JsonIgnore
    @Override
    public TaskStatus getStatusFromExistingStatus(TaskStatus status, ZonedDateTime startTime, ZonedDateTime taskEndTime, String taskParentId, String taskName) {
        return this.status;
    }

    public TaskStatus getStatus() {
        return this.status;
    }

    public void setStatus(TaskStatus status) {
        this.status = status;
    }

    @JsonIgnore
    @Override
    public boolean isStartEvent(){
        return true;
    }
}

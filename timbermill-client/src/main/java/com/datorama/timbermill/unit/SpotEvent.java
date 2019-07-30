package com.datorama.timbermill.unit;

import javax.validation.constraints.NotNull;

import java.time.ZonedDateTime;

public class SpotEvent extends Event {
    private final Task.TaskStatus status;

    public SpotEvent(String name, @NotNull LogParams logParams, String primaryId, String parentId, Task.TaskStatus status) {
        super(null, name, logParams, parentId);
        if (primaryId == null){
            this.primaryId = taskId;
        } else {
            this.primaryId = primaryId;
        }
        this.status = status;
    }

    public ZonedDateTime getEndTime() {
        return time;
    }

    @Override
    public Task.TaskStatus getStatus(Task.TaskStatus status) {
        return this.status;
    }

    @Override
    public boolean isStartEvent(){
        return true;
    }
}

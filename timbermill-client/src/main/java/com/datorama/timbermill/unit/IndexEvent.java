package com.datorama.timbermill.unit;

import java.time.ZonedDateTime;

public class IndexEvent {
    private final String name = "timbermill_index";

    private final Integer eventsAmount;
    private final Integer fetchedAmount;

    private TaskMetaData meta = new TaskMetaData();

    private final Task.TaskStatus status;
    private final String exception;
    private final Long pluginsDuration;
    public IndexEvent(Integer eventsAmount, Integer fetchedAmount, ZonedDateTime startTime, ZonedDateTime endTime, long indexerDuration, Task.TaskStatus status, String exception, Long pluginsDuration) {
        this.eventsAmount = eventsAmount;
        this.fetchedAmount = fetchedAmount;
        this.status = status;
        this.exception = exception;
        this.pluginsDuration = pluginsDuration;

        meta.setTaskBegin(startTime);
        meta.setTaskEnd(endTime);
        meta.setDuration(indexerDuration);
    }
    public String getName() {
        return name;
    }

    public Integer getEventsAmount() {
        return eventsAmount;
    }

    public Integer getFetchedAmount() {
        return fetchedAmount;
    }

    public Task.TaskStatus getStatus() {
        return status;
    }

    public String getException() {
        return exception;
    }

    public Long getPluginsDuration() {
        return pluginsDuration;
    }

    public TaskMetaData getMeta() {
        return meta;
    }
}

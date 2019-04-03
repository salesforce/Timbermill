package com.datorama.timbermill.unit;

import java.time.ZonedDateTime;

public class IndexEvent {
    private final String name = "timbermill_index";
    private final Integer eventsAmount;
    private final Integer fetchedAmount;
    private final ZonedDateTime startTime;
    private final ZonedDateTime endTime;
    private final long indexerDuration;
    private final Task.TaskStatus status;
    private final String exception;
    private Long pluginsDuration;

    public IndexEvent(Integer eventsAmount, Integer fetchedAmount, ZonedDateTime startTime, ZonedDateTime endTime, long indexerDuration, Task.TaskStatus status, String exception, Long pluginsDuration) {
        this.eventsAmount = eventsAmount;
        this.fetchedAmount = fetchedAmount;
        this.startTime = startTime;
        this.endTime = endTime;
        this.indexerDuration = indexerDuration;
        this.status = status;
        this.exception = exception;
        this.pluginsDuration = pluginsDuration;
    }

    public Integer getEventsAmount() {
        return eventsAmount;
    }

    public Integer getFetchedAmount() {
        return fetchedAmount;
    }

    public ZonedDateTime getStartTime() {
        return startTime;
    }

    public ZonedDateTime getEndTime() {
        return endTime;
    }

    public long getIndexerDuration() {
        return indexerDuration;
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
}

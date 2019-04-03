package com.datorama.timbermill.unit;

import com.datorama.timbermill.unit.Task;

import java.time.ZonedDateTime;

public class PluginApplierEvent {
    private final String name = "timbermill_plugin";
    private final ZonedDateTime startTime;
    private final String pluginName;
    private final String pluginClass;
    private final Task.TaskStatus status;
    private final String exception;
    private final ZonedDateTime endTime;
    private final long duration;

    public PluginApplierEvent(ZonedDateTime startTime, String pluginName, String pluginClass, Task.TaskStatus status, String exception, ZonedDateTime endTime, long duration) {
        this.startTime = startTime;
        this.pluginName = pluginName;
        this.pluginClass = pluginClass;
        this.status = status;
        this.exception = exception;
        this.endTime = endTime;
        this.duration = duration;
    }

    public String getName() {
        return name;
    }

    public ZonedDateTime getStartTime() {
        return startTime;
    }

    public String getPluginName() {
        return pluginName;
    }

    public String getPluginClass() {
        return pluginClass;
    }

    public Task.TaskStatus getStatus() {
        return status;
    }

    public String getException() {
        return exception;
    }

    public ZonedDateTime getEndTime() {
        return endTime;
    }

    public long getDuration() {
        return duration;
    }

}

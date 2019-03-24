package com.datorama.timbermill.unit;

import java.time.ZonedDateTime;

class TaskMetaData {
    private ZonedDateTime taskBegin;
    private ZonedDateTime taskEnd;
    private Long duration;

    ZonedDateTime getTaskBegin() {
        return taskBegin;
    }

    void setTaskBegin(ZonedDateTime taskBegin) {
        this.taskBegin = taskBegin;
    }

    ZonedDateTime getTaskEnd() {
        return taskEnd;
    }

    void setTaskEnd(ZonedDateTime taskEnd) {
        this.taskEnd = taskEnd;
    }

    void setDuration(Long duration) {
        this.duration = duration;
    }

    long getDuration() {
        return duration;
    }
}

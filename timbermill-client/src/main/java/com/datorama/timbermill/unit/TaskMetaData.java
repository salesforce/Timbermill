package com.datorama.timbermill.unit;

import org.joda.time.DateTime;

class TaskMetaData {
    private DateTime taskBegin;
    private DateTime taskEnd;
    private long totalDuration;

    DateTime getTaskBegin() {
        return taskBegin;
    }

    void setTaskBegin(DateTime taskBegin) {

        this.taskBegin = taskBegin;
    }

    DateTime getTaskEnd() {
        return taskEnd;
    }

    void setTaskEnd(DateTime taskEnd) {
        this.taskEnd = taskEnd;
    }

    void setTotalDuration(long totalDuration) {
        this.totalDuration = totalDuration;
    }

    long getTotalDuration() {
        return totalDuration;
    }
}

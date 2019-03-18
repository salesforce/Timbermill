package com.datorama.timbermill.unit;

import org.joda.time.DateTime;

class TaskMetaData {
    private String env;
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

    String getEnv() {
        return env;
    }

    void setEnv(String env) {
        this.env = env;
    }

    void setTotalDuration(long totalDuration) {
        this.totalDuration = totalDuration;
    }

    long getTotalDuration() {
        return totalDuration;
    }
}

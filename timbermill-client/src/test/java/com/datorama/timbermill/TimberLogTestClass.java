package com.datorama.timbermill;

import com.datorama.timbermill.annotation.TimberLog;


class TimberLogTestClass {

    private final String taskId;

    @TimberLog(name = "ctr")
    TimberLogTestClass() {
        taskId = TimberLogger.getCurrentTaskId();
    }

    @TimberLog(name = "ctr")
    TimberLogTestClass(String[] task) throws Exception {
        task[0] = TimberLogger.getCurrentTaskId();
        throw new Exception();
    }

    String getTaskId() {
        return taskId;
    }
}

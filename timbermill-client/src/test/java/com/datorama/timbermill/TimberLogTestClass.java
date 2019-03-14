package com.datorama.timbermill;

import com.datorama.timbermill.annotation.TimberLog;


public class TimberLogTestClass {

    private final String taskId;

    @TimberLog(taskType = "ctr")
    public TimberLogTestClass() {
        taskId = TimberLogger.getCurrentTaskId();
    }

    @TimberLog(taskType = "ctr")
    public TimberLogTestClass(String[] task) throws Exception {
        task[0] = TimberLogger.getCurrentTaskId();
        throw new Exception();
    }

    public String getTaskId() {
        return taskId;
    }
}

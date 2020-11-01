package com.datorama.oss.timbermill.unit;

import java.util.Map;

import com.datorama.oss.timbermill.ClientHeartbeater;

public class HeartbeatTask extends Task {

    private static final String THREAD_NAME = "threadName";
    public HeartbeatTask(Event e, long daysRotation) {
        setStartTime(e.getTime());
        setName(e.getName());
        setStatus(TaskStatus.SUCCESS);

        setDateToDelete(e.getDateToDelete(daysRotation));

        Map<String, String> eventString = e.getStrings();
        Map<String, String> taskContext = this.getCtx();
        taskContext.put(THREAD_NAME, eventString.get(THREAD_NAME));
        setEnv(e.getEnv());

        Map<String, Number> eventMetrics = e.getMetrics();
        Map<String, Number> taskMetrics = this.getMetric();
        if (eventMetrics != null) {
            taskMetrics.put("submitAmount", eventMetrics.get(ClientHeartbeater.SUBMIT_AMOUNT));
            taskMetrics.put("avgSubmitDuration", eventMetrics.get(ClientHeartbeater.AVG_SUBMIT_DURATION));
            taskMetrics.put("maxSubmitDuration", eventMetrics.get(ClientHeartbeater.MAX_SUBMIT_DURATION));
            taskMetrics.put("outputBufferSize", eventMetrics.get(ClientHeartbeater.OUTPUT_BUFFER_SIZE));
        }
    }
}

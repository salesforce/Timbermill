package com.datorama.timbermill.unit;

import java.util.Map;

import static com.datorama.timbermill.ClientHeartbeater.*;

public class HeartbeatTask extends Task{

    private static final String THREAD_NAME = "threadName";
    public HeartbeatTask(Event e, long daysRotation) {
        setStartTime(e.getTime());
        setName(e.getName());
        setStatus(e.getStatusFromExistingStatus(null));

        setDateToDelete(e.getDateToDelete(daysRotation));

        Map<String, String> eventContext = e.getContext();
        Map<String, String> taskContext = this.getCtx();
        taskContext.put(THREAD_NAME, eventContext.get(THREAD_NAME));
        setEnv(e.getEnv());

        Map<String, Number> eventMetrics = e.getMetrics();
        Map<String, Number> taskMetrics = this.getMetric();
        if (eventMetrics != null) {
            taskMetrics.put("submitAmount", eventMetrics.get(SUBMIT_AMOUNT));
            taskMetrics.put("avgSubmitDuration", eventMetrics.get(AVG_SUBMIT_DURATION));
            taskMetrics.put("maxSubmitDuration", eventMetrics.get(MAX_SUBMIT_DURATION));
            taskMetrics.put("outputBufferSize", eventMetrics.get(OUTPUT_BUFFER_SIZE));
        }
    }
}

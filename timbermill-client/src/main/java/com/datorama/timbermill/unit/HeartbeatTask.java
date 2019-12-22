package com.datorama.timbermill.unit;

import java.time.ZonedDateTime;
import java.util.Map;

import static com.datorama.timbermill.ClientHeartbeater.*;

public class HeartbeatTask {
    private TaskMetaData meta = new TaskMetaData();

    private final String name;

    private final String env;
    private final String threadName;
    private final Number submitAmount;
    private final Number avgSubmitDuration;
    private final Number maxSubmitDuration;
    private final Number outputBufferSize;
    public HeartbeatTask(Event e) {
        this.meta.setTaskBegin(e.getTime());
        this.name = e.getName();

        Map<String, String> context = e.getContext();
        this.env = e.getEnv();
        this.threadName = context.get("threadName");

        Map<String, Number> metrics = e.getMetrics();
        this.submitAmount = metrics.get(SUBMIT_AMOUNT);
        this.avgSubmitDuration = metrics.get(AVG_SUBMIT_DURATION);
        this.maxSubmitDuration = metrics.get(MAX_SUBMIT_DURATION);
        this.outputBufferSize = metrics.get(OUTPUT_BUFFER_SIZE);
    }

    public TaskMetaData getMeta() {
        return meta;
    }

    public String getName() {
        return name;
    }

    public String getEnv() {
        return env;
    }

    public String getThreadName() {
        return threadName;
    }

    public Number getSubmitAmount() {
        return submitAmount;
    }

    public Number getAvgSubmitDuration() {
        return avgSubmitDuration;
    }

    public Number getMaxSubmitDuration() {
        return maxSubmitDuration;
    }

    public Number getOutputBufferSize() {
        return outputBufferSize;
    }
}

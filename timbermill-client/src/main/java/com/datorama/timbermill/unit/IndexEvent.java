package com.datorama.timbermill.unit;

import java.time.ZonedDateTime;
import java.util.Map;

import com.datorama.timbermill.common.TimbermillUtils;

import static com.datorama.timbermill.TaskIndexer.getTimesDuration;
import static com.datorama.timbermill.common.Constants.EXCEPTION;

public class IndexEvent extends Task{

    private IndexEvent(String env, Integer fetchedAmount, ZonedDateTime startTime, ZonedDateTime endTime, TaskStatus status, Integer eventsAmount, String exception, long defaultDaysRotation) {
        setName("metadata_timbermill_index");
        setEnv(env);
        Map<String, Number> metric = getMetric();
        metric.put("eventsAmount", eventsAmount);
        metric.put("fetchedAmount", fetchedAmount);
        setStatus(status);
        if (exception != null){
            getText().put(EXCEPTION, exception);
        }
        setStartTime(startTime);
        setEndTime(endTime);
        long indexerDuration = getTimesDuration(startTime, endTime);
        setDuration(indexerDuration);
        setDateToDelete(TimbermillUtils.getDefaultDateToDelete(defaultDaysRotation));
    }

    public IndexEvent(String env, Integer fetchedAmount, ZonedDateTime startTime, ZonedDateTime endTime, Integer eventsAmount, long defaultDaysRotation) {
        this(env, fetchedAmount, startTime, endTime, TaskStatus.SUCCESS, eventsAmount, null, defaultDaysRotation);
    }

    public IndexEvent(String env, Integer fetchedAmount, ZonedDateTime startTime, ZonedDateTime endTime, String exception, Integer eventsAmount, long defaultDaysRotation) {
        this(env, fetchedAmount, startTime, endTime, TaskStatus.ERROR, eventsAmount, exception, defaultDaysRotation);
    }
}

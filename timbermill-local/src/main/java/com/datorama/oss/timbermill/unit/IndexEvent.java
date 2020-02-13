package com.datorama.oss.timbermill.unit;

import java.time.ZonedDateTime;
import java.util.Map;

import com.datorama.oss.timbermill.common.Constants;
import com.datorama.oss.timbermill.common.ElasticsearchUtil;
import com.datorama.oss.timbermill.common.TimbermillDatesUtils;

public class IndexEvent extends Task {

    private IndexEvent(String env, Integer fetchedAmount, ZonedDateTime startTime, ZonedDateTime endTime, TaskStatus status, Integer eventsAmount, String exception, long defaultDaysRotation) {
        setName("metadata_timbermill_index");
        setEnv(env);
        Map<String, Number> metric = getMetric();
        metric.put("eventsAmount", eventsAmount);
        metric.put("fetchedAmount", fetchedAmount);
        setStatus(status);
        if (exception != null){
            getText().put(Constants.EXCEPTION, exception);
        }
        setStartTime(startTime);
        setEndTime(endTime);
        long indexerDuration = ElasticsearchUtil.getTimesDuration(startTime, endTime);
        setDuration(indexerDuration);
        setDateToDelete(TimbermillDatesUtils.getDateToDeleteWithDefault(defaultDaysRotation));
    }

    public IndexEvent(String env, Integer fetchedAmount, ZonedDateTime startTime, ZonedDateTime endTime, Integer eventsAmount, long defaultDaysRotation) {
        this(env, fetchedAmount, startTime, endTime, TaskStatus.SUCCESS, eventsAmount, null, defaultDaysRotation);
    }
}

package com.datorama.oss.timbermill.unit;

import java.time.ZonedDateTime;
import java.util.Map;

import com.datorama.oss.timbermill.common.TimbermillDatesUtils;

public class IndexEvent extends Task {

    public IndexEvent(String env, Integer fetchedAmount, ZonedDateTime startTime, ZonedDateTime endTime, Integer eventsAmount, long defaultDaysRotation,
            long timesDuration) {
        setName("metadata_timbermill_index");
        setEnv(env);
        Map<String, Number> metric = getMetric();
        metric.put("eventsAmount", eventsAmount);
        metric.put("fetchedAmount", fetchedAmount);
        setStatus(TaskStatus.SUCCESS);

        setStartTime(startTime);
        setEndTime(endTime);
        setDuration(timesDuration);
        setDateToDelete(TimbermillDatesUtils.getDateToDeleteWithDefault(defaultDaysRotation));
    }

}

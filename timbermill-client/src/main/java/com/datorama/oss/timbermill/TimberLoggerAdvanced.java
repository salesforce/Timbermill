package com.datorama.oss.timbermill;

import java.time.ZonedDateTime;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

import com.datorama.oss.timbermill.unit.LogParams;

public class TimberLoggerAdvanced {
    private static final Logger LOG = LoggerFactory.getLogger(TimberLoggerAdvanced.class);

    public static String start(String name) {
        return start(name, LogParams.create());
    }

    public static String start(String name, LogParams logParams) {
        return start(name, null, logParams);
    }

    public static String start(String name, String parentTaskId) {
        return start(name, parentTaskId, LogParams.create());
    }

    public static String start(String name, String parentTaskId, LogParams logParams) {
        return start(null, name, parentTaskId, logParams);
    }

    public static String start(String taskId, String name, String parentTaskId, LogParams logParams) {
        return startWithDateToDelete(taskId, name, parentTaskId, logParams,null);
    }

    public static String startWithDaysToKeep(String name, int daysToKeep) {
        return startWithDaysToKeep(name, (LogParams) null, daysToKeep);
    }

    public static String startWithDaysToKeep(String name, LogParams logParams, int daysToKeep) {
        return startWithDaysToKeep(name, null, logParams, daysToKeep);
    }

    public static String startWithDaysToKeep(String name, String parentTaskId, int daysToKeep) {
        return startWithDaysToKeep(name, parentTaskId, null, daysToKeep);
    }

    public static String startWithDaysToKeep(String name, String parentTaskId, LogParams logParams, int daysToKeep) {
        return startWithDateToDelete(null, name, parentTaskId, logParams, TimberLogger.createDateToDelete(daysToKeep));
    }

    public static String startWithDateToDelete(String name, ZonedDateTime dateToDelete) {
        return startWithDateToDelete(name, (LogParams) null, dateToDelete);
    }

    public static String startWithDateToDelete(String name, LogParams logParams, ZonedDateTime dateToDelete) {
        return startWithDateToDelete(name, null, logParams, dateToDelete);
    }

    public static String startWithDateToDelete(String name, String parentTaskId, ZonedDateTime dateToDelete) {
        return startWithDateToDelete(name, parentTaskId, null, dateToDelete);
    }

    public static String startWithDateToDelete(String name, String parentTaskId, LogParams logParams, ZonedDateTime dateToDelete) {
        return startWithDateToDelete(null, name, parentTaskId, logParams, dateToDelete);
    }

    private static String startWithDateToDelete(String taskId, String name, String parentTaskId, LogParams logParams, ZonedDateTime dateToDelete) {
        return EventLogger.get().startEvent(taskId, name, parentTaskId, logParams, true, dateToDelete);
    }

    public static String logParams(@NotNull String ongoingTaskId, LogParams logParams) {
        if (StringUtils.isEmpty(ongoingTaskId)){
            LOG.error("`ongoingTaskId` can't be empty, ignoring TimberLoggerAdvanced `logParams` method call");
        }
        return EventLogger.get().logParams(logParams, ongoingTaskId);
    }

    public static void success(@NotNull String ongoingTaskId) {
        success(ongoingTaskId, LogParams.create());
    }

    public static void success(@NotNull String ongoingTaskId, LogParams logParams) {
        if (StringUtils.isEmpty(ongoingTaskId)){
            LOG.error("`ongoingTaskId` can't be empty, ignoring TimberLoggerAdvanced `success` method call");
        }
        EventLogger.get().successEvent(ongoingTaskId, logParams);
    }

    public static void error(@NotNull String ongoingTaskId, Throwable t) {
        error(ongoingTaskId, t, null);
    }

    public static void error(@NotNull String ongoingTaskId, Throwable t, LogParams logParams) {
        if (StringUtils.isEmpty(ongoingTaskId)){
            LOG.error("`ongoingTaskId` can't be empty, ignoring TimberLoggerAdvanced `error` method call");
        }
        EventLogger.get().endWithError(t, ongoingTaskId, logParams);
    }



}

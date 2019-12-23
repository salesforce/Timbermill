package com.datorama.timbermill;

import java.time.ZonedDateTime;

import com.datorama.timbermill.unit.LogParams;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

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

    static String start(String taskId, String name, String parentTaskId, LogParams logParams) {
        return EventLogger.get().startEvent(taskId, name, parentTaskId, logParams, true, null);
    }

    public static String startWithDateToDelete(String name, ZonedDateTime dateToDelete) {
        return startWithDateToDelete(name, LogParams.create(), dateToDelete);
    }

    public static String startWithDateToDelete(String name, LogParams logParams, ZonedDateTime dateToDelete) {
        return startWithDateToDelete(name, null, logParams, dateToDelete);
    }

    public static String startWithDateToDelete(String name, String parentTaskId, ZonedDateTime dateToDelete) {
        return startWithDateToDelete(name, parentTaskId, LogParams.create(), dateToDelete);
    }

    public static String startWithDateToDelete(String name, String parentTaskId, LogParams logParams, ZonedDateTime dateToDelete) {
        return EventLogger.get().startEvent(null, name, parentTaskId, logParams, true, dateToDelete);
    }

    //For testing only
    public static String logParams(@NotNull String ongoingTaskId, LogParams logParams) {
        if (StringUtils.isEmpty(ongoingTaskId)){
            LOG.error("`ongoingTaskId` can't be empty, ignoring TimberLoggerAdvanced `logParams` method call");
        }
        if (logParams == null){
            logParams = LogParams.create();
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
        error(ongoingTaskId, t, LogParams.create());
    }

    public static void error(@NotNull String ongoingTaskId, Throwable t, LogParams logParams) {
        if (StringUtils.isEmpty(ongoingTaskId)){
            LOG.error("`ongoingTaskId` can't be empty, ignoring TimberLoggerAdvanced `error` method call");
        }
        if (logParams == null){
            logParams = LogParams.create();
        }
        EventLogger.get().endWithError(t, ongoingTaskId, logParams);
    }



}

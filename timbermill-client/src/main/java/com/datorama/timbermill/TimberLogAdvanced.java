package com.datorama.timbermill;

import com.datorama.timbermill.unit.LogParams;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotNull;

public class TimberLogAdvanced {

    public static String start(String name) {
        return start(name, LogParams.create());
    }

    public static String start(String name, LogParams logParams) {
        return start(name, null, logParams);
    }

    public static String start(String name, String parentTaskId) {
        return start(name, parentTaskId, LogParams.create());
    }

    public static String start(String name, String parentTaskId, @NotNull LogParams logParams) {
        return EventLogger.get().startEvent(name, parentTaskId, logParams, true);
    }

    public static String logParams(@NotNull String ongoingTaskId, @NotNull LogParams logParams) {
        if (StringUtils.isEmpty(ongoingTaskId)){
            throw new RuntimeException("ongoingTaskId can't be empty");
        }
        return EventLogger.get().logParams(logParams, ongoingTaskId);
    }

    public static void success(@NotNull String ongoingTaskId) {
        if (StringUtils.isEmpty(ongoingTaskId)){
            throw new RuntimeException("ongoingTaskId can't be empty");
        }
        EventLogger.get().successEvent(ongoingTaskId);
    }

    public static void error(@NotNull String ongoingTaskId, Throwable t) {
        if (StringUtils.isEmpty(ongoingTaskId)){
            throw new RuntimeException("ongoingTaskId can't be empty");
        }
        EventLogger.get().endWithError(t, ongoingTaskId);
    }

}

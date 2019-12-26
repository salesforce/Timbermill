package com.datorama.timbermill;

import com.datorama.timbermill.pipe.EventOutputPipe;
import com.datorama.timbermill.unit.LogParams;
import com.datorama.timbermill.unit.Task;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.exception.ExceptionUtils;

import javax.validation.constraints.NotNull;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Function;

import static com.datorama.timbermill.common.Constants.EXCEPTION;

public final class TimberLogger {

	private static final String DEFAULT = "default";

	private TimberLogger() {
	}

	public static void bootstrap(EventOutputPipe pipe) {
		bootstrap(pipe, Maps.newHashMap(), DEFAULT);
	}

	public static void bootstrap(EventOutputPipe pipe, String env) {
		bootstrap(pipe, Maps.newHashMap(), env);
	}

	public static void bootstrap(EventOutputPipe pipe, Map<String, String> staticParams, String env) {
		if (env == null){
			env = DEFAULT;
		}
		EventLogger.bootstrap(pipe, true, staticParams, env);
	}

    public static void exit() {
		EventLogger.exit();
	}

	/*
	 * Return null if stack is empty
	 */
	public static String getCurrentTaskId() {
		return EventLogger.get().getCurrentTaskId();
	}

	public static String start(String name) {
		return start(name, null);
	}

	public static String start(String name, LogParams logParams) {
		return start(name, null, logParams);
	}

	public static String start(String name, String parentTaskId, LogParams logParams) {
		return startWithDateToDelete(name, parentTaskId, logParams, null);
	}

	public static String startWithDaysToKeep(String name, int daysToKeep) {
		return startWithDaysToKeep(name, null, daysToKeep);
	}

	public static String startWithDaysToKeep(String name, LogParams logParams, int daysToKeep) {
		return startWithDaysToKeep(name, null, logParams, daysToKeep);
	}

	public static String startWithDaysToKeep(String name, String parentTaskId, LogParams logParams, int daysToKeep) {
		return startWithDateToDelete(name, parentTaskId, logParams, createDateToDelete(daysToKeep));
	}

	public static String startWithDateToDelete(String name, ZonedDateTime dateToDelete) {
		return startWithDateToDelete(name, null, dateToDelete);
	}

	public static String startWithDateToDelete(String name, LogParams logParams, ZonedDateTime dateToDelete) {
		return startWithDateToDelete(name, null, logParams, dateToDelete);
	}

	public static String startWithDateToDelete(String name, String parentTaskId, LogParams logParams, ZonedDateTime dateToDelete) {
		return EventLogger.get().startEvent(name, parentTaskId, logParams, dateToDelete);
	}

	public static String success() {
		return EventLogger.get().successEvent();
	}

	public static String error(Throwable t) {
		return EventLogger.get().endWithError(t);
	}

	public static String logParams(LogParams logParams) {
		return EventLogger.get().logParams(logParams);
	}

	public static String logString(String key, Object value) {
		return EventLogger.get().logParams(LogParams.create().string(key, value));
	}

	public static String logContext(String key, Object value) {
		return EventLogger.get().logParams(LogParams.create().context(key, value));
	}

	public static String logMetric(String key, Number value) {
		return EventLogger.get().logParams(LogParams.create().metric(key, value));
	}

	public static String logText(String key, String value) {
		return EventLogger.get().logParams(LogParams.create().text(key, value));
	}

    public static String logInfo(String log) {
        return EventLogger.get().logParams(LogParams.create().logInfo(log));
    }

	public static String logWarn(String log) {
		return EventLogger.get().logParams(LogParams.create().logWarn(log));
	}

	public static String logError(String log) {
		return EventLogger.get().logParams(LogParams.create().logError(log));
	}

	public static String spot(String name) {
		return spot(name, null);
	}

	public static String spot(String name, LogParams logParams) {
		return spotWithDateToDelete(name, logParams, null);
	}

	public static String spotWithDaysToKeep(String name, int daysToKeep) {
		return spotWithDaysToKeep(name, null, daysToKeep);
	}

	public static String spotWithDaysToKeep(String name, LogParams logParams, int daysToKeep) {
		return spotWithDateToDelete(name, logParams, createDateToDelete(daysToKeep));
	}

	public static String spotWithDateToDelete(String name, ZonedDateTime dateToDelete) {
		return spotWithDateToDelete(name, null, dateToDelete);
	}

	public static String spotWithDateToDelete(String name, LogParams logParams, ZonedDateTime dateToDelete) {
		return EventLogger.get().spotEvent(name, logParams, Task.TaskStatus.SUCCESS, dateToDelete);
	}

	public static String spotError(String name, Throwable t) {
		return spotError(name, null, t);
	}

	public static String spotError(String name, LogParams logParams, Throwable t) {
		return spotErrorWithDateToDelete(name, logParams, t, null);
	}

	public static String spotErrorWithDaysToKeep(String name, Throwable t, int daysToKeep) {
		return spotErrorWithDaysToKeep(name, null, t, daysToKeep);
	}

	public static String spotErrorWithDaysToKeep(String name, LogParams logParams, Throwable t, int daysToKeep) {
		return spotErrorWithDateToDelete(name, logParams, t, createDateToDelete(daysToKeep));
	}

	public static String spotErrorWithDateToDelete(String name, Throwable t, ZonedDateTime dateToDelete ) {
		return spotErrorWithDateToDelete(name, null, t, dateToDelete);
	}

	public static String spotErrorWithDateToDelete(String name, LogParams logParams, Throwable t, ZonedDateTime dateToDelete) {
		if (logParams == null){
			logParams = LogParams.create();
		}
		if (t != null) {
			logParams.text(EXCEPTION, t + "\n" + ExceptionUtils.getStackTrace(t));
		}
		return EventLogger.get().spotEvent(name, logParams, Task.TaskStatus.ERROR, dateToDelete);
	}

	public static <T> Callable<T> wrapCallable(Callable<T> callable) {
		return EventLogger.get().wrapCallable(callable);
	}

	public static <T,R> Function<T,R> wrapFunctional(Function<T, R> function){
		return EventLogger.get().wrapFunction(function);
	}

	static ZonedDateTime createDateToDelete(int daysToKeep) {
		ZonedDateTime dateToDelete = ZonedDateTime.now();
		if (daysToKeep > 0){
			dateToDelete = dateToDelete.plusDays(daysToKeep);
		}
		return dateToDelete;
	}
}

package com.datorama.oss.timbermill;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Function;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.oss.timbermill.common.Constants;
import com.datorama.oss.timbermill.pipe.EventOutputPipe;
import com.datorama.oss.timbermill.unit.LogParams;
import com.datorama.oss.timbermill.unit.TaskStatus;

public final class TimberLogger {

	private static final Logger LOG = LoggerFactory.getLogger(TimberLogger.class);

	private TimberLogger() {
	}

	public static void bootstrap(EventOutputPipe pipe) {
		bootstrap(pipe, new HashMap<>(), Constants.DEFAULT);
	}

	public static void bootstrap(EventOutputPipe pipe, String env) {
		bootstrap(pipe, new HashMap<>(), env);
	}

	public static void bootstrap(EventOutputPipe pipe, Map<String, String> staticParams, String env) {
		if (env == null){
			env = Constants.DEFAULT;
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
		return start(name, null, null);
	}

	public static String start(String name, LogParams logParams) {
		return start(name, null, logParams);
	}

	public static String start(String name, String parentTaskId) {
		return start(name, parentTaskId, null);
	}

	public static String start(String name, String parentTaskId, LogParams logParams) {
		return start(null, name, parentTaskId, logParams);
	}

	static String start(String taskId, String name, String parentTaskId, LogParams logParams) {
		return startWithDateToDelete(taskId, name, parentTaskId, logParams, null);
	}

	public static String startWithDaysToKeep(String name, int daysToKeep) {
		return startWithDaysToKeep(name, LogParams.create(), daysToKeep);
	}

	public static String startWithDaysToKeep(String name, LogParams logParams, int daysToKeep) {
		return startWithDaysToKeep(name, null, logParams, daysToKeep);
	}

	public static String startWithDaysToKeep(String name, String parentTaskId, int daysToKeep) {
		return startWithDaysToKeep(name, parentTaskId, null, daysToKeep);
	}

	public static String startWithDaysToKeep(String name, String parentTaskId, LogParams logParams, int daysToKeep) {
		return startWithDaysToKeep(null, name, parentTaskId, logParams, daysToKeep);
	}

	static String startWithDaysToKeep(String taskId, String name, String parentTaskId, LogParams logParams, int daysToKeep) {
		return startWithDateToDelete(taskId, name, parentTaskId, logParams, createDateToDelete(daysToKeep));
	}

	public static String startWithDateToDelete(String name, ZonedDateTime dateToDelete) {
		return startWithDateToDelete(name, LogParams.create(), dateToDelete);
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

	static String startWithDateToDelete(String taskId, String name, String parentTaskId, LogParams logParams, ZonedDateTime dateToDelete) {
		return EventLogger.get().startEvent(taskId, name, parentTaskId, logParams, false, dateToDelete);
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

	public static String success() {
		return success(LogParams.create());
	}

	public static String success(LogParams logParams) {
		return EventLogger.get().successEvent(null, logParams);
	}

	public static String error() {
		return error(LogParams.create());
	}

	public static String error(LogParams logParams) {
		return error(null, logParams);
	}

	public static String error(Throwable t) {
		return error(t, LogParams.create());
	}

	public static String error(Throwable t, LogParams logParams) {
		return EventLogger.get().endWithError(t, null, logParams);
	}

	public static String spot(String name) {
		return spot(name, null);
	}

	public static String spot(String name, LogParams logParams) {
		return spot(name, null, logParams);
	}

	public static String spot(String name, String parentTaskId, LogParams logParams) {
		return spot(null, name, parentTaskId, logParams);
	}

	static String spot(String taskId, String name, String parentTaskId, LogParams logParams) {
		return spotWithDateToDelete(taskId, name, parentTaskId, logParams, null);
	}

	public static String spotWithDaysToKeep(String name, int daysToKeep) {
		return spotWithDaysToKeep(name, null, daysToKeep);
	}

	public static String spotWithDaysToKeep(String name, LogParams logParams, int daysToKeep) {
		return spotWithDaysToKeep(name, null, logParams, daysToKeep);
	}

	public static String spotWithDaysToKeep(String name, String parentTaskId, LogParams logParams, int daysToKeep) {
		return spotWithDaysToKeep(null, name, parentTaskId, logParams, daysToKeep);
	}

	static String spotWithDaysToKeep(String taskId, String name, String parentTaskId, LogParams logParams, int daysToKeep) {
		return spotWithDateToDelete(taskId, name, parentTaskId, logParams, createDateToDelete(daysToKeep));
	}

	public static String spotWithDateToDelete(String name, ZonedDateTime dateToDelete) {
		return spotWithDateToDelete(name, null, dateToDelete);
	}

	public static String spotWithDateToDelete(String name, LogParams logParams, ZonedDateTime dateToDelete) {
		return spotWithDateToDelete(name, null, logParams, dateToDelete);
	}

	public static String spotWithDateToDelete(String name, String parentTaskId, LogParams logParams, ZonedDateTime dateToDelete) {
		return spotWithDateToDelete(null, name, parentTaskId, logParams, dateToDelete);
	}

	static String spotWithDateToDelete(String taskId, String name, String parentTaskId, LogParams logParams, ZonedDateTime dateToDelete) {
		return EventLogger.get().spotEvent(taskId, name, parentTaskId, logParams, TaskStatus.SUCCESS, dateToDelete);
	}

	public static String spotError(String name, Throwable t) {
		return spotError(name, null, t);
	}

	public static String spotError(String name, LogParams logParams, Throwable t) {
		return spotError(name, null, logParams, t);
	}

	public static String spotError(String name, String parentTaskId, LogParams logParams, Throwable t) {
		return spotErrorWithDateToDelete(name, parentTaskId, logParams, t, null);
	}

	public static String spotErrorWithDaysToKeep(String name, Throwable t, int daysToKeep) {
		return spotErrorWithDaysToKeep(name, null, t, daysToKeep);
	}

	public static String spotErrorWithDaysToKeep(String name, LogParams logParams, Throwable t, int daysToKeep) {
		return spotErrorWithDaysToKeep(name, null, logParams, t, daysToKeep);
	}

	public static String spotErrorWithDaysToKeep(String name, String parentTaskId, LogParams logParams, Throwable t, int daysToKeep) {
		return spotErrorWithDateToDelete(name, parentTaskId, logParams, t, createDateToDelete(daysToKeep));
	}

	public static String spotErrorWithDateToDelete(String name, Throwable t, ZonedDateTime dateToDelete ) {
		return spotErrorWithDateToDelete(name, null, t, dateToDelete);
	}

	public static String spotErrorWithDateToDelete(String name, LogParams logParams, Throwable t, ZonedDateTime dateToDelete) {
		return spotErrorWithDateToDelete(name, null, logParams, t, dateToDelete);
	}

	private static String spotErrorWithDateToDelete(String name, String parentTaskId, LogParams logParams, Throwable t, ZonedDateTime dateToDelete) {
		if (logParams == null){
			logParams = LogParams.create();
		}
		if (t != null) {
			logParams.text(Constants.EXCEPTION, t + "\n" + ExceptionUtils.getStackTrace(t));
		}
		return EventLogger.get().spotEvent(null, name, parentTaskId, logParams, TaskStatus.ERROR, dateToDelete);
	}

	public static <T> Callable<T> wrapCallable(Callable<T> callable) {
		return EventLogger.get().wrapCallable(callable);
	}

	public static <T,R> Function<T,R> wrapFunctional(Function<T, R> function){
		return EventLogger.get().wrapFunction(function);
	}

	public static void clear(){
		EventLogger.get().clearStack();
	}

	static ZonedDateTime createDateToDelete(int daysToKeep) {
		ZonedDateTime dateToDelete = ZonedDateTime.now();
		if (daysToKeep > 0){
			try {
				dateToDelete = dateToDelete.plusDays(daysToKeep);
			} catch (Throwable t){
				LOG.error("Error parsing date", t);
			}
		}
		return dateToDelete;
	}
}

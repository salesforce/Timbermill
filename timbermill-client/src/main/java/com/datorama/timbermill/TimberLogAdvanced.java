package com.datorama.timbermill;

import org.joda.time.DateTime;

import java.util.Collections;

public final class TimberLogAdvanced {

	public static String start(String taskType) {
		return start(taskType, LogParams.create());
	}

	public static String start(String taskType, LogParams logParams) {
		return start(taskType, null, logParams);
	}

	public static String start(String taskType, String parentTaskId) {
		return start(taskType, parentTaskId, LogParams.create());
	}

	public static String start(String taskType, String parentTaskId, LogParams logParams) {
		return EventLogger.get().startEvent(taskType, parentTaskId, new DateTime(), logParams.getAttributes(),
				logParams.getMetrics(), logParams.getData(), true);
	}

	public static void logParams(String ongoingTaskId, LogParams logParams) {
		EventLogger.get().logAttributes(logParams.getAttributes(), ongoingTaskId);
		EventLogger.get().logData(logParams.getData(), ongoingTaskId);
		EventLogger.get().logMetrics(logParams.getMetrics(), ongoingTaskId);
	}

	public static void logAttributes(String ongoingTaskId, String key, Object value) {
		EventLogger.get().logAttributes(Collections.singletonMap(key, value), ongoingTaskId);
	}

	public static void logMetrics(String ongoingTaskId, String key, Number value) {
		EventLogger.get().logMetrics(Collections.singletonMap(key, value), ongoingTaskId);
	}

	public static void logData(String ongoingTaskId, String key, String value) {
		EventLogger.get().logData(Collections.singletonMap(key, value), ongoingTaskId);
	}

	public static void success(String ongoingTaskId) {
		EventLogger.get().successEvent(ongoingTaskId);
	}

	public static void error(String ongoingTaskId, Throwable t) {
		EventLogger.get().endWithError(t, ongoingTaskId);
	}

	public static TaskContext withContext(String ongoingTaskId) {
		return new TaskContext(ongoingTaskId);
	}
}

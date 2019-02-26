package com.datorama.timbermill;

import org.joda.time.DateTime;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Function;

public final class TimberLog {

	private TimberLog() {
	}

	public static void bootstrap(Map<String, Object> staticParams, EventOutputPipe eventOutputPipe) {
		EventLogger.bootstrap(staticParams, eventOutputPipe, true);
	}

	public static void stop() {
		EventLogger.stop();
	}

	@Deprecated
	public static void logAroundWithThrowable(String taskType, RunnableTask runnableTask) throws Throwable{
		start(taskType);
		try {
			runnableTask.run();
		} catch (Throwable t) {
			error(t);
			throw t;
		}
		success();
	}

	@Deprecated
	public static void logAroundWithException(String taskType, RunnableTask runnableTask) throws Exception {
		start(taskType);
		try {
			runnableTask.run();
		} catch (Exception e) {
			error(e);
			throw e;
		}
		success();
	}

	public static void logAround(String taskType, Runnable runnableTask) {
		start(taskType);
		try {
			runnableTask.run();
		} catch (RuntimeException e) {
			error(e);
			throw e;
		}
		success();
	}

	public static void logAround(String taskType, Runnable runnableTask, LogParams logParams) {
		start(taskType, logParams);
		try {
			runnableTask.run();
		} catch (RuntimeException e) {
			error(e);
			throw e;
		}
		success();
	}

	@Deprecated
	public static void logAroundWithThrowable(String taskType, DateTime predefinedTime, RunnableTask runnableTask) throws Throwable {
		start(taskType, predefinedTime);
		try {
			runnableTask.run();
		} catch (Throwable t) {
			error(t);
			throw t;
		}
		success();
	}

	@Deprecated
	public static void logAroundWithException(String taskType, DateTime predefinedTime, RunnableTask runnableTask) throws Exception {
		start(taskType, predefinedTime);
		try {
			runnableTask.run();
		} catch (Exception e) {
			error(e);
			throw e;
		}
		success();
	}

	public static void logAround(String taskType, DateTime predefinedTime, Runnable runnableTask) {
		start(taskType, predefinedTime);
		try {
			runnableTask.run();
		} catch (RuntimeException e) {
			error(e);
			throw e;
		}
		success();
	}

	@Deprecated
	public static void logAroundWithThrowable(String taskType, String parentTaskId, RunnableTask runnableTask) throws Throwable {
		start(taskType, parentTaskId);
		try {
			runnableTask.run();
		} catch (Throwable t) {
			error(t);
			throw t;
		}
		success();
	}

	@Deprecated
	public static void logAroundWithException(String taskType, String parentTaskId, RunnableTask runnableTask) throws Exception {
		start(taskType, parentTaskId);
		try {
			runnableTask.run();
		} catch (Exception e) {
			error(e);
			throw e;
		}
		success();
	}

	public static void logAround(String taskType, String parentTaskId, Runnable runnableTask) {
		start(taskType, parentTaskId);
		try {
			runnableTask.run();
		} catch (RuntimeException e) {
			error(e);
			throw e;
		}
		success();
	}

	public static void logAround(String taskType, String parentTaskId, Runnable runnableTask, LogParams logParams) {
		start(taskType, parentTaskId, logParams);
		try {
			runnableTask.run();
		} catch (RuntimeException e) {
			error(e);
			throw e;
		}
		success();
	}

	@Deprecated
	public static <T> T logAroundWithThrowable(String taskType, String parentTaskId, Callable<T> callable) throws Throwable{
		T ret;
		start(taskType, parentTaskId);
		try {
			ret = callable.call();
		} catch (Throwable t) {
			error(t);
			throw t;
		}
		success();
		return ret;
	}

	@Deprecated
	public static <T> T logAroundWithException(String taskType, String parentTaskId, Callable<T> callable) throws Exception {
		T ret;
		start(taskType, parentTaskId);
		try {
			ret = callable.call();
		} catch (Exception e) {
			error(e);
			throw e;
		}
		success();
		return ret;
	}

	public static <T> T logAround(String taskType, String parentTaskId, CallableTask<T> callable) {
		T ret;
		start(taskType, parentTaskId);
		try {
			ret = callable.call();
		} catch (RuntimeException e) {
			error(e);
			throw e;
		}
		success();
		return ret;
	}

	public static <T> T logAround(String taskType, String parentTaskId, CallableTask<T> callable, LogParams logParams) {
		T ret;
		start(taskType, parentTaskId, logParams);
		try {
			ret = callable.call();
		} catch (RuntimeException e) {
			error(e);
			throw e;
		}
		success();
		return ret;
	}

	@Deprecated
	public static <T> T logAroundWithThrowable(String taskType, Callable<T> callable) throws Throwable {
		T ret;
		start(taskType);
		try {
			ret = callable.call();
		} catch (Throwable t) {
			error(t);
			throw t;
		}
		success();
		return ret;
	}

	@Deprecated
	public static <T> T logAroundWithException(String taskType, Callable<T> callable) throws Exception {
		T ret;
		start(taskType);
		try {
			ret = callable.call();
		} catch (Exception e) {
			error(e);
			throw e;
		}
		success();
		return ret;
	}

	public static <T> T logAround(String taskType, CallableTask<T> callable) {
		T ret = null;
		start(taskType);
		try {
			ret = callable.call();
		} catch (RuntimeException e) {
			error(e);
			throw e;
		}
		success();
		return ret;
	}

	public static <T> T logAround(String taskType, CallableTask<T> callable, LogParams logParams) {
		T ret = null;
		start(taskType, logParams);
		try {
			ret = callable.call();
		} catch (RuntimeException e) {
			error(e);
			throw e;
		}
		success();
		return ret;
	}

	/*
	 * Return null if stack is empty
	 */
	public static String getCurrentTaskId() {
		return EventLogger.get().getCurrentTaskId();
	}

	public static String start(String taskType) {
		return start(taskType, new DateTime());
	}

	public static String start(String taskType, LogParams logParams) {
		return start(taskType, null, logParams);
	}

	public static String start(String taskType, String parentTaskId, LogParams logParams) {
		return EventLogger.get().startEvent(taskType, parentTaskId, new DateTime(), logParams.getAttributes(), logParams.getMetrics(), logParams.getData(), false);
	}

	public static String start(String taskType, DateTime time) {
		return EventLogger.get().startEvent(taskType, null, time, false);
	}

	private static String start(String taskType, DateTime time, String parentTaskId) {
		return EventLogger.get().startEvent(taskType, parentTaskId, time,  false);
	}

	public static String start(String taskType, String parentTaskId) {
		return EventLogger.get().startEvent(taskType, parentTaskId, new DateTime(),  false);
	}

	public static String success() {
		return success(new DateTime());
	}

	public static String spot(String taskType, Params<?> attributes, Params<Number> metrics, Params<String> data) {
		return spot(taskType, attributes, metrics, data, null);
	}

	public static String spot(String taskType, Params<?> attributes, Params<Number> metrics, Params<String> data, String parentTaskId) {
		return EventLogger.get().spotEvent(taskType, (attributes != null) ? attributes.getMap() : null,
				(metrics != null) ? metrics.getMap() : null,
				(data != null) ? data.getMap() : null, parentTaskId);
	}

	public static String spot(String taskType, LogParams logParams) {
		return spot(taskType, logParams, null);
	}

	public static String spot(String taskType, LogParams logParams, String parentTaskId) {
		return EventLogger.get().spotEvent(taskType, logParams.getAttributes(), logParams.getMetrics(), logParams.getData(), parentTaskId);
	}

	public static String error(Throwable t) {
		return error(t, new DateTime());
	}

	public static String appError(Throwable t) {
		return appError(t, new DateTime());
	}

	public static String logParams(LogParams logParams) {
		return EventLogger.get().logParams(logParams);
	}

	public static String logAttributes(Params<Object> params) {
		return EventLogger.get().logAttributes(params.getMap());
	}

	public static String logAttributes(String key, Object value) {
		return logAttributes(Params.createObjectParams().put(key, value));
	}

	public static String logMetrics(Params<Number> params) {
		return EventLogger.get().logMetrics(params.getMap());
	}

	public static String logMetrics(String key, Number value) {
		return logMetrics(Params.createNumberParams().put(key, value));
	}

	public static String logData(Params<Object> params) {
		return EventLogger.get().logData(params.getMap());
	}

	public static String logData(String key, String value2) {
		return logData(Params.createObjectParams().put(key, value2));
	}

	public static <T> Callable<T> wrapCallable(Callable<T> callable) {
		return EventLogger.get().wrapCallable(callable);
	}

	public static <T,R> Function<T,R> wrapFunctional(Function<T, R> function){
		return EventLogger.get().wrapFunction(function);
	}


	public static String success(DateTime time) {
		return EventLogger.get().successEvent(time, null);
	}

	public static String error(Throwable t, DateTime time) {
		return EventLogger.get().endWithError(t, time, null);
	}

	public static String appError(Throwable t, DateTime time) {
		return EventLogger.get().endWithAppError(t, time);
	}

}

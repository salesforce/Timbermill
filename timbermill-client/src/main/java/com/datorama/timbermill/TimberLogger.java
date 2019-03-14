package com.datorama.timbermill;

import com.datorama.timbermill.pipe.LocalOutputPipe;
import com.datorama.timbermill.pipe.LocalOutputPipeConfig;
import org.joda.time.DateTime;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Function;

public final class TimberLogger {

	private TimberLogger() {
	}

	public static void bootstrap() {
		LocalOutputPipeConfig.Builder builder = new LocalOutputPipeConfig.Builder();
		LocalOutputPipeConfig config = builder.url("http://localhost:9200").build();
		bootstrap(config);
	}

	public static void bootstrap(LocalOutputPipeConfig config) {
		LocalOutputPipe pipe = new LocalOutputPipe(config);
		EventLogger.bootstrap(config.getStaticParams(), pipe, true);
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

	public static String start(String taskType) {
		return start(taskType, null);
	}

	public static String start(String taskType, LogParams logParams) {
		Map<String, Object> attributes = null;
		Map<String, Number> metrics = null;
		Map<String, String> data = null;
		if (logParams != null) {
			attributes = logParams.getAttributes();
			metrics = logParams.getMetrics();
			data = logParams.getData();
		}
		return EventLogger.get().startEvent(taskType, attributes, metrics, data);
	}

	public static String success() {
		return EventLogger.get().successEvent(new DateTime());
	}

	public static String error(Throwable t) {
		return EventLogger.get().endWithError(t, new DateTime());
	}

	public static String logParams(LogParams logParams) {
		return EventLogger.get().logParams(logParams);
	}

	public static String logAttributes(String key, Object value) {
		return EventLogger.get().logAttributes(Collections.singletonMap(key, value));
	}

	public static String logMetrics(String key, Number value) {
		return EventLogger.get().logMetrics(Collections.singletonMap(key, value));
	}

	public static String logData(String key, String value) {
		return EventLogger.get().logData(Collections.singletonMap(key, value));
	}

	public static String spot(String taskType) {
		return spot(taskType, null);
	}

	private static String spot(String taskType, LogParams logParams) {
		Map<String, Object> attributes = null;
		Map<String, Number> metrics = null;
		Map<String, String> data = null;
		if (logParams != null) {
			attributes = logParams.getAttributes();
			metrics = logParams.getMetrics();
			data = logParams.getData();
		}
		return EventLogger.get().spotEvent(taskType, attributes, metrics, data);
	}

	public static <T> Callable<T> wrapCallable(Callable<T> callable) {
		return EventLogger.get().wrapCallable(callable);
	}

	public static <T,R> Function<T,R> wrapFunctional(Function<T, R> function){
		return EventLogger.get().wrapFunction(function);
	}
}

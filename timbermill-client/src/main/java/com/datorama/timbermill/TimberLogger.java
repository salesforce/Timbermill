package com.datorama.timbermill;

import com.datorama.timbermill.pipe.LocalOutputPipe;
import com.datorama.timbermill.pipe.LocalOutputPipeConfig;
import org.joda.time.DateTime;

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

	public static String start(String name) {
		return start(name, null);
	}

	static String start(String name, LogParams logParams) {
		if (logParams == null){
			logParams = LogParams.create();
		}
		return EventLogger.get().startEvent(name, logParams.getStrings(), logParams.getTexts(), logParams.getGlobals(), logParams.getMetrics());
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

	public static String logString(String key, Object value) {
		return EventLogger.get().logParams(LogParams.create().string(key, value));
	}

	public static String logGlobal(String key, Object value) {
		return EventLogger.get().logParams(LogParams.create().global(key, value));
	}

	public static String logMetric(String key, Number value) {
		return EventLogger.get().logParams(LogParams.create().metric(key, value));
	}

	public static String logText(String key, String value) {
		return EventLogger.get().logParams(LogParams.create().text(key, value));
	}

	public static String spot(String name) {
		return spot(name, null);
	}

	private static String spot(String name, LogParams logParams) {
		Map<String, Object> strings = null;
		Map<String, String> texts = null;
		Map<String, Object> globals = null;
		Map<String, Number> metrics = null;
		if (logParams != null) {
			strings = logParams.getStrings();
			globals = logParams.getGlobals();
			metrics = logParams.getMetrics();
			texts = logParams.getTexts();
		}
		return EventLogger.get().spotEvent(name, strings, texts, globals, metrics);
	}

	public static <T> Callable<T> wrapCallable(Callable<T> callable) {
		return EventLogger.get().wrapCallable(callable);
	}

	public static <T,R> Function<T,R> wrapFunctional(Function<T, R> function){
		return EventLogger.get().wrapFunction(function);
	}
}

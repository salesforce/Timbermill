package com.datorama.timbermill;

import com.datorama.timbermill.pipe.LocalOutputPipe;
import com.datorama.timbermill.pipe.LocalOutputPipeConfig;

import javax.validation.constraints.NotNull;
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
		return EventLogger.get().startEvent(name, logParams);
	}

	public static String success() {
		return EventLogger.get().successEvent();
	}

	public static String error(Throwable t) {
		return EventLogger.get().endWithError(t);
	}

	public static String logParams(@NotNull LogParams logParams) {
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
		if (logParams == null){
			logParams = LogParams.create();
		}
		return EventLogger.get().spotEvent(name, logParams);
	}

	public static <T> Callable<T> wrapCallable(Callable<T> callable) {
		return EventLogger.get().wrapCallable(callable);
	}

	public static <T,R> Function<T,R> wrapFunctional(Function<T, R> function){
		return EventLogger.get().wrapFunction(function);
	}
}

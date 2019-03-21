package com.datorama.timbermill;

import com.datorama.timbermill.common.Constants;
import com.datorama.timbermill.pipe.BlackHolePipe;
import com.datorama.timbermill.pipe.BufferingOutputPipe;
import com.datorama.timbermill.pipe.EventOutputPipe;
import com.datorama.timbermill.pipe.StatisticsCollectorOutputPipe;
import com.datorama.timbermill.unit.*;
import com.google.common.collect.ImmutableMap.Builder;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.function.Function;

import static com.datorama.timbermill.common.Constants.EXCEPTION;

final class EventLogger {
	private static final Logger LOG = LoggerFactory.getLogger(EventLogger.class);

	/**
	 * Factory related fields
	 */
	private static Map<String, String> staticParams = Collections.emptyMap();
	private static ThreadLocal<EventLogger> threadInstance = ThreadLocal.withInitial(() -> new EventLogger(new BlackHolePipe()));
	private static boolean isBootstrapped;

	/**
	 * Instance fields
	 */
	private Stack<String> taskIdStack = new Stack<>();
	private EventOutputPipe eventOutputPipe;

	private EventLogger(EventOutputPipe eventOutputPipe) {
		this.eventOutputPipe = eventOutputPipe;
	}

	static void bootstrap(Map<String, String> staticParameters, EventOutputPipe eventOutputPipe, boolean doHeartbeat) {
		if (isBootstrapped) {
			LOG.warn("EventLogger is already bootstrapped, ignoring this bootstrap invocation. EventOutputPipe={}, ({})", eventOutputPipe, staticParameters);
		} else {
			LOG.info("Bootstrapping EventLogger with params ({})", eventOutputPipe, staticParameters);
			isBootstrapped = true;
			BufferingOutputPipe bufferingOutputPipe = new BufferingOutputPipe(eventOutputPipe);
			bufferingOutputPipe.start();

			StatisticsCollectorOutputPipe statsCollector = new StatisticsCollectorOutputPipe(bufferingOutputPipe);

			if (doHeartbeat) {
				ClientHeartbeater heartbeater = new ClientHeartbeater(statsCollector, bufferingOutputPipe);
				heartbeater.start();
			}
			staticParams = new Builder<String, String>().putAll(staticParameters).build();

			threadInstance = ThreadLocal.withInitial(() -> new EventLogger(statsCollector));
		}
	}

	static void exit(){
		threadInstance.get().eventOutputPipe.close();
		isBootstrapped = false;
	}

	static EventLogger get() {
		return threadInstance.get();
	}


	/*
	 * Maps are never null
	 */
	String startEvent(String name, @NotNull LogParams logParams) {
		addStaticParams(logParams);
		Event event = createStartEvent(name, logParams);
		return submitEvent(event);
	}

	String successEvent() {
		Event event = createSuccessEvent();
		return submitEvent(event);
	}

	private Event createSuccessEvent() {
		LogParams logParams = LogParams.create();
		Event e;
		if (taskIdStack.empty()) {
			e = getCorruptedEvent(logParams);
		} else {
			String taskId = taskIdStack.pop();
			e = new SuccessEvent(taskId, logParams);
		}
		return e;
	}

	String endWithError(Throwable t) {
		Event event = createErrorEvent(t);
		return submitEvent(event);
	}

	private Event createErrorEvent(Throwable t) {
		LogParams logParams = LogParams.create();
		if (t != null) {
			logParams.text(EXCEPTION, t + "\n" + ExceptionUtils.getStackTrace(t));
		}
		Event e;
		if (taskIdStack.empty()) {
			e = getCorruptedEvent(logParams);
		} else {
			String taskId = taskIdStack.pop();
			e = new ErrorEvent(taskId, logParams);
		}
		return e;
	}

	String spotEvent(String name, @NotNull LogParams logParams) {
		Event event = createSpotEvent(name, logParams);
		return submitEvent(event);
	}

	String logParams(@NotNull LogParams logParams) {
		Event event = createInfoEvent(logParams);
		return submitEvent(event);
	}

	String getCurrentTaskId() {
		return taskIdStack.empty() ? null : taskIdStack.peek();
	}

	<T> Callable<T> wrapCallable(Callable<T> callable) {
		final Stack<String> origTaskIdStack = taskIdStack;
		return () -> {
			get().taskIdStack = (Stack<String>) origTaskIdStack.clone();
			T call = callable.call();
			get().taskIdStack.clear();
			return call;
		};
	}

	<T, R> Function<T, R> wrapFunction(Function<T, R> function) {
		final Stack<String> origTaskIdStack = taskIdStack;
		return t -> {
			get().taskIdStack = (Stack<String>) origTaskIdStack.clone();
			R call = function.apply(t);
			get().taskIdStack.clear();
			return call;
		};
	}

	void addIdToContext(String ongoingTaskId) {
		taskIdStack.push(ongoingTaskId);
	}

	void removeIdFromContext(String ongoingTaskId) {
		if (!taskIdStack.isEmpty() && taskIdStack.peek().equals(ongoingTaskId)){
			taskIdStack.pop();
		}
		else{
			throw new RuntimeException("Task id opened with TimberlogAdvanced.withContext() is not the top of the stack, probably failed to closed all the tasks in the scope");
		}
	}

	private Event createStartEvent(String name, @NotNull LogParams logParams) {
		PrimaryParentIdPair primaryParentIdPair = getPrimaryParentIdPair();
		Event event = new StartEvent(name, logParams, primaryParentIdPair.getPrimaryId(), primaryParentIdPair.getParentId());
		taskIdStack.push(event.getTaskId());
		return event;
	}

	private Event createInfoEvent(@NotNull LogParams logParams) {
		Event e;
		if (taskIdStack.empty()) {
			e = getCorruptedEvent(logParams);
		} else {
			e = new InfoEvent(taskIdStack.peek(), logParams);
		}
		return e;
	}

	private Event createSpotEvent(String name, @NotNull LogParams logParams) {
		addStaticParams(logParams);
		PrimaryParentIdPair primaryParentIdPair = getPrimaryParentIdPair();
		return new SpotEvent(name, logParams, primaryParentIdPair.getPrimaryId(), primaryParentIdPair.getParentId());
	}

	private PrimaryParentIdPair getPrimaryParentIdPair() {
		String primaryId = null;
		String parentId = null;
		if (!taskIdStack.isEmpty()) {
			primaryId = taskIdStack.firstElement();
			parentId = taskIdStack.peek();
		}
		return new PrimaryParentIdPair(primaryId, parentId);
	}

	private void addStaticParams(@NotNull LogParams logParams) {
		logParams.context("threadName", Thread.currentThread().getName());
		logParams.context(staticParams);
	}

	private Event getCorruptedEvent(@NotNull LogParams logParams) {
		Event e;
		String stackTrace = getStackTraceString();
		logParams.text("stackTrace", stackTrace);
		e = createSpotEvent(Constants.LOG_WITHOUT_CONTEXT, logParams);
		return e;
	}

	private static String getStackTraceString() {
		StringBuilder sb = new StringBuilder();
		StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
		stackTrace = Arrays.copyOfRange(stackTrace, 5, 10);
		for (StackTraceElement stackTraceElement : stackTrace) {
			sb.append(stackTraceElement).append('\n');
		}
		return sb.toString();
	}

	private String submitEvent(Event event) {
		eventOutputPipe.send(event);
		return event.getTaskId();
	}

	private class PrimaryParentIdPair {
		private final String primaryId;

		private final String parentId;

		PrimaryParentIdPair(String primaryId, String parentId) {

			this.primaryId = primaryId;
			this.parentId = parentId;
		}
		String getPrimaryId() {
			return primaryId;
		}

		String getParentId() {
			return parentId;
		}
	}
}
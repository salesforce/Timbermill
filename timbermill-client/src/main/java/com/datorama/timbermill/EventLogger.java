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
    private static final String THREAD_NAME = "threadName";

    /**
	 * Factory related fields
	 */
	private static Map<String, String> staticParams = Collections.emptyMap();
	private static ThreadLocal<EventLogger> threadInstance = ThreadLocal.withInitial(() -> new EventLogger(new BlackHolePipe()));
	private static boolean isBootstrapped;
	private static String env;

	/**
	 * Instance fields
	 */
	private Stack<String> taskIdStack = new Stack<>();
	private EventOutputPipe eventOutputPipe;

	private EventLogger(EventOutputPipe eventOutputPipe) {
		this.eventOutputPipe = eventOutputPipe;
	}

	static void bootstrap(EventOutputPipe eventOutputPipe, boolean doHeartbeat, Map<String, String> staticParams, String environment) {
		env = environment;
		if (isBootstrapped) {
			LOG.warn("EventLogger is already bootstrapped, ignoring this bootstrap invocation. EventOutputPipe={}, ({})", eventOutputPipe, staticParams);
		} else {
			LOG.info("Bootstrapping EventLogger with params ({})", eventOutputPipe, staticParams);
			isBootstrapped = true;
			BufferingOutputPipe bufferingOutputPipe = new BufferingOutputPipe(eventOutputPipe);
			bufferingOutputPipe.start();

			StatisticsCollectorOutputPipe statsCollector = new StatisticsCollectorOutputPipe(bufferingOutputPipe);

			if (doHeartbeat) {
				ClientHeartbeater heartbeater = new ClientHeartbeater(statsCollector, bufferingOutputPipe);
				heartbeater.start();
			}
			EventLogger.staticParams = new Builder<String, String>().putAll(staticParams).build();

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
		return startEvent(null, name, null ,logParams, false);
	}

	String startEvent(String name, String parentTaskId, LogParams logParams) {
		return startEvent(null, name, parentTaskId ,logParams, false);
	}

	String startEvent(String taskId, String name, String parentTaskId, LogParams logParams, boolean isOngoingTask) {
		addStaticParams(logParams);
		Event event = createStartEvent(taskId, logParams, parentTaskId, isOngoingTask, name);
		return submitEvent(event);
	}

	String successEvent() {
		return successEvent(null, LogParams.create());
	}

	String successEvent(String ongoingTaskId, LogParams logParams) {
		Event event = createSuccessEvent(ongoingTaskId, logParams);
		return submitEvent(event);
	}

	private Event createSuccessEvent(String ongoingTaskId, LogParams logParams) {
		Event e;
		if (ongoingTaskId == null) {
			if (taskIdStack.empty()) {
				e = getCorruptedEvent(logParams, Task.TaskStatus.CORRUPTED_SUCCESS);
			} else {
				e = new SuccessEvent(taskIdStack.pop(), logParams);
			}
		}
		else {
			e = new SuccessEvent(ongoingTaskId, logParams);
		}
		return e;
	}

	String endWithError(Throwable t) {
		return endWithError(t, null, LogParams.create());
	}

    String endWithError(Throwable t, String ongoingTaskId, @NotNull LogParams logParams) {
        Event event = createErrorEvent(t, ongoingTaskId, logParams);
        return submitEvent(event);
    }

	private Event createErrorEvent(Throwable t, String ongoingTaskId, @NotNull LogParams logParams) {
		if (t != null) {
			logParams.text(EXCEPTION, t + "\n" + ExceptionUtils.getStackTrace(t));
		}
		Event e;
		if (ongoingTaskId == null) {
			if (taskIdStack.empty()) {
				e = getCorruptedEvent(logParams, Task.TaskStatus.CORRUPTED_ERROR);
			} else {
				e = new ErrorEvent(taskIdStack.pop(), logParams);
			}
		}
		else{
			e = new ErrorEvent(ongoingTaskId, logParams);
		}
		return e;
	}

	String spotEvent(String name, @NotNull LogParams logParams, Task.TaskStatus status) {
		Event event = createSpotEvent(name, logParams, status);
		return submitEvent(event);
	}

	String logParams(@NotNull LogParams logParams) {
		return logParams(logParams, null);
	}

	public String logParams(@NotNull LogParams logParams, String ongoingTaskId) {
		Event event = createInfoEvent(logParams, ongoingTaskId);
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
            LOG.error("Task id: {} opened with TimberlogAdvanced.withContext() is not the top of the stack, probably failed to closed all the tasks in the scope", ongoingTaskId);
		}
	}

	private Event createStartEvent(String taskId, @NotNull LogParams logParams, String parentTaskId, boolean isOngoingTask, String name) {
		PrimaryParentIdPair primaryParentIdPair;
		Event event;
		if (!isOngoingTask) {
			primaryParentIdPair = getPrimaryParentIdPair(parentTaskId);
			event = new StartEvent(taskId, name, logParams, primaryParentIdPair.getPrimaryId(), primaryParentIdPair.getParentId());
			taskIdStack.push(event.getTaskId());
		}
		else{
			event = new StartEvent(taskId, name, logParams, null, parentTaskId);
		}
		return event;
	}

	private Event createInfoEvent(@NotNull LogParams logParams, String ongoingTaskId) {
		Event e;
		if (ongoingTaskId == null) {
			if (taskIdStack.empty()) {
				e = getCorruptedEvent(logParams, Task.TaskStatus.CORRUPTED);
			} else {
				e = new InfoEvent(taskIdStack.peek(), logParams);
			}
		}
		else{
			e = new InfoEvent(ongoingTaskId, logParams);
		}
		return e;
	}

	private Event createSpotEvent(String name, @NotNull LogParams logParams, Task.TaskStatus status) {
		addStaticParams(logParams);
		PrimaryParentIdPair primaryParentIdPair = getPrimaryParentIdPair(null);
		return new SpotEvent(name, logParams, primaryParentIdPair.getPrimaryId(), primaryParentIdPair.getParentId(), status);
	}

	private PrimaryParentIdPair getPrimaryParentIdPair(String parentTaskId) {
		if (parentTaskId != null){
			return new PrimaryParentIdPair(null, parentTaskId);
		}
		String primaryId = null;
		String parentId = null;
		if (!taskIdStack.isEmpty()) {
			primaryId = taskIdStack.firstElement();
			parentId = taskIdStack.peek();
		}
		return new PrimaryParentIdPair(primaryId, parentId);
	}

	private void addStaticParams(@NotNull LogParams logParams) {
		logParams.context(THREAD_NAME, Thread.currentThread().getName());
		logParams.context(staticParams);
	}

	private Event getCorruptedEvent(@NotNull LogParams logParams, Task.TaskStatus status) {
		String stackTrace = getStackTraceString();
		logParams.text("stackTrace", stackTrace);
		return createSpotEvent(Constants.LOG_WITHOUT_CONTEXT, logParams, status);
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
		event.setEnv(env);
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
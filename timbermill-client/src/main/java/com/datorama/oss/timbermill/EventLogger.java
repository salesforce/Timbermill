package com.datorama.oss.timbermill;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.function.Function;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.oss.timbermill.common.Constants;
import com.datorama.oss.timbermill.pipe.BlackHolePipe;
import com.datorama.oss.timbermill.pipe.EventOutputPipe;
import com.datorama.oss.timbermill.pipe.StatisticsCollectorOutputPipe;
import com.datorama.oss.timbermill.unit.*;

final class EventLogger {
	public static final String LOG_WITHOUT_CONTEXT = "LogWithoutContext";
	private static final Logger LOG = LoggerFactory.getLogger(EventLogger.class);
    private static final String THREAD_NAME = "threadName";
	static final String STACK_TRACE = "stackTrace";

	/**
	 * Factory related fields
	 */
	private static Map<String, String> staticParams = new HashMap<>();
	private static ThreadLocal<EventLogger> threadInstance = ThreadLocal.withInitial(() -> new EventLogger(new BlackHolePipe()));
	private static boolean isBootstrapped;
	private static String env;

	/**
	 * Instance fields
	 */
	private Stack<String> taskIdStack = new Stack<>();
	private final EventOutputPipe eventOutputPipe;

	private EventLogger(EventOutputPipe eventOutputPipe) {
		this.eventOutputPipe = eventOutputPipe;
	}

	static void bootstrap(EventOutputPipe eventOutputPipe, boolean doHeartbeat, Map<String, String> staticParams, String environment) {
		env = environment;
		if (isBootstrapped) {
			LOG.warn("EventLogger is already bootstrapped, ignoring this bootstrap invocation. EventOutputPipe={}, ({})", eventOutputPipe, staticParams);
		} else {
			LOG.info("Bootstrapping EventLogger with params ({})", staticParams);
			isBootstrapped = true;
			StatisticsCollectorOutputPipe statsCollector = new StatisticsCollectorOutputPipe(eventOutputPipe);

			if (doHeartbeat) {
				ClientHeartbeater heartbeater = new ClientHeartbeater(statsCollector, eventOutputPipe);
				heartbeater.start();
			}
			EventLogger.staticParams.putAll(staticParams);

			threadInstance = ThreadLocal.withInitial(() -> new EventLogger(statsCollector));
		}
	}

	static void exit(){
		isBootstrapped = false;
	}

	static EventLogger get() {
		return threadInstance.get();
	}


	String startEvent(String name, LogParams logParams) {
		return startEvent(null, name, null ,logParams, false, null);
	}

	String startEvent(String taskId, String name, String parentTaskId, LogParams logParams, boolean isOngoingTask, ZonedDateTime dateToDelete) {
		if (logParams == null) {
			logParams = LogParams.create();
		}
		addStaticParams(logParams);
		Event event = createStartEvent(taskId, logParams, parentTaskId, isOngoingTask, name, dateToDelete);
		return submitEvent(event);
	}

	String successEvent() {
		return successEvent(null, LogParams.create());
	}

	String successEvent(String ongoingTaskId, LogParams logParams) {
		if (logParams == null){
			logParams = LogParams.create();
		}
		Event event = createSuccessEvent(ongoingTaskId, logParams);
		return submitEvent(event);
	}

	String endWithError(Throwable t) {
		return endWithError(t, null, null);
	}

    String endWithError(Throwable t, String ongoingTaskId, LogParams logParams) {
		if (logParams == null){
			logParams = LogParams.create();
		}
		Event event = createErrorEvent(t, ongoingTaskId, logParams);
        return submitEvent(event);
    }

	String spotEvent(String taskId, String name, String parentTaskId, LogParams logParams, TaskStatus status, ZonedDateTime dateToDelete) {
		if (logParams == null){
			logParams = LogParams.create();
		}
		Event event = createSpotEvent(taskId, name, logParams, status, dateToDelete, parentTaskId);
		return submitEvent(event);
	}

	String logParams(LogParams logParams) {
		return logParams(logParams, null);
	}

	String logParams(LogParams logParams, String ongoingTaskId) {
		if (logParams == null){
			logParams = LogParams.create();
		}
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

	private Event createStartEvent(String taskId, LogParams logParams, String parentTaskId, boolean isOngoingTask, String name, ZonedDateTime dateToDelete) {
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
		setDateToDelete(dateToDelete, event);
		return event;
	}

	private Event createSuccessEvent(String ongoingTaskId, LogParams logParams) {
		Event e;
		if (ongoingTaskId == null) {
			if (taskIdStack.empty()) {
				e = getCorruptedEvent(logParams);
			} else {
				e = new SuccessEvent(taskIdStack.pop(), logParams);
			}
		}
		else {
			e = new SuccessEvent(ongoingTaskId, logParams);
		}
		return e;
	}

	private Event createErrorEvent(Throwable t, String ongoingTaskId, LogParams logParams) {
		if (t != null) {
			logParams.text(Constants.EXCEPTION, t + "\n" + ExceptionUtils.getStackTrace(t));
		}
		Event e;
		if (ongoingTaskId == null) {
			if (taskIdStack.empty()) {
				e = getCorruptedEvent(logParams);
			} else {
				e = new ErrorEvent(taskIdStack.pop(), logParams);
			}
		}
		else{
			e = new ErrorEvent(ongoingTaskId, logParams);
		}
		return e;
	}

	private Event createInfoEvent(LogParams logParams, String ongoingTaskId) {
		Event e;
		if (ongoingTaskId == null) {
			if (taskIdStack.empty()) {
				e = getCorruptedEvent(logParams);
			} else {
				e = new InfoEvent(taskIdStack.peek(), logParams);
			}
		}
		else{
			e = new InfoEvent(ongoingTaskId, logParams);
		}
		return e;
	}

	private Event createSpotEvent(String taskId, String name, LogParams logParams, TaskStatus status, ZonedDateTime dateToDelete, String parentTaskId) {
		if (logParams == null){
			logParams = LogParams.create();
		}
		addStaticParams(logParams);
		PrimaryParentIdPair primaryParentIdPair = getPrimaryParentIdPair(parentTaskId);
		SpotEvent spotEvent = new SpotEvent(taskId, name, primaryParentIdPair.getPrimaryId(), primaryParentIdPair.getParentId(), status, logParams);
		setDateToDelete(dateToDelete, spotEvent);
		return spotEvent;
	}

	private void setDateToDelete(ZonedDateTime dateToDelete, Event event) {
		if (dateToDelete != null){
			ZonedDateTime now = ZonedDateTime.now();
			if (dateToDelete.isBefore(now)){
				dateToDelete = now;
			}
			event.setDateToDelete(dateToDelete);
		}
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
		logParams.string(THREAD_NAME, Thread.currentThread().getName());
		logParams.string(staticParams);
	}

	private Event getCorruptedEvent(@NotNull LogParams logParams) {
		String stackTrace = getStackTraceString();
		logParams.text(STACK_TRACE, stackTrace);
		return createSpotEvent(null, LOG_WITHOUT_CONTEXT, logParams, TaskStatus.CORRUPTED, null, null);
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

	private static class PrimaryParentIdPair {
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
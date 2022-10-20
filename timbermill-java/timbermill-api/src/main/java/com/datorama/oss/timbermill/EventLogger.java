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
		try {
		env = environment;
			if (isBootstrapped) {
				LOG.warn("EventLogger is already bootstrapped, ignoring this bootstrap invocation. EventOutputPipe={}, ({})", eventOutputPipe, staticParams);
			} else {
				LOG.info("Timbermill 2 client 26072020");
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
		} catch (Throwable t){
			LOG.error("Was unable to bootstrap Timbermill", t);
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
		try {
			addStaticParams(logParams);
			Event event = createStartEvent(taskId, logParams, parentTaskId, isOngoingTask, name, dateToDelete);
			return submitEvent(event);
		} catch (Throwable throwable){
			LOG.error("Was unable to send event to Timbermill", throwable);
			return null;
		}
	}

	void successEvent(String taskId) {
		if (!taskIdStack.isEmpty()) {
			String latestTaskId = taskIdStack.peek();
			if (latestTaskId != null && latestTaskId.equals(taskId)) {
				successEvent();
			}
		}
	}

	String successEvent() {
		return successEvent(null, LogParams.create());
	}

	String successEvent(String ongoingTaskId, LogParams logParams) {
		if (logParams == null){
			logParams = LogParams.create();
		}
		try{
			Event event = createSuccessEvent(ongoingTaskId, logParams);
			return submitEvent(event);
		} catch (Throwable throwable){
			LOG.error("Was unable to send event to Timbermill", throwable);
			return null;
		}
	}

	void endWithError(String taskId, Throwable t) {
		if (!taskIdStack.isEmpty()) {
			String latestTaskId = taskIdStack.peek();
			if (latestTaskId != null && latestTaskId.equals(taskId)) {
				endWithError(t);
			}
		}
	}

	String endWithError(Throwable t) {
		return endWithError(t, null, null);
	}

    String endWithError(Throwable t, String ongoingTaskId, LogParams logParams) {
		if (logParams == null){
			logParams = LogParams.create();
		}
		try{
			Event event = createErrorEvent(t, ongoingTaskId, logParams);
			return submitEvent(event);
		} catch (Throwable throwable){
			LOG.error("Was unable to send event to Timbermill", throwable);
			return null;
		}
    }

	String spotEvent(String taskId, String name, String parentTaskId, LogParams logParams, TaskStatus status, ZonedDateTime dateToDelete) {
		if (logParams == null){
			logParams = LogParams.create();
		}
		try{
			Event event = createSpotEvent(taskId, name, logParams, status, dateToDelete, parentTaskId);
			return submitEvent(event);
		} catch (Throwable throwable){
			LOG.error("Was unable to send event to Timbermill", throwable);
			return null;
		}
	}

	String logParams(LogParams logParams) {
		return logParams(logParams, null);
	}

	String logParams(LogParams logParams, String ongoingTaskId) {
		if (logParams == null){
			logParams = LogParams.create();
		}
		try{
			Event event = createInfoEvent(logParams, ongoingTaskId);
			return submitEvent(event);
		} catch (Throwable throwable){
			LOG.error("Was unable to send event to Timbermill", throwable);
			return null;
		}
	}

	String getCurrentTaskId() {
		return taskIdStack.empty() ? null : taskIdStack.peek();
	}

	private class Scope implements AutoCloseable {
		final Stack<String> origStack = taskIdStack;

		Scope(String taskId) {
			taskIdStack = new Stack<String>();
			if(null != taskId)
				taskIdStack.push(taskId);
		}

		<T> T apply(Callable<T> c) throws Exception {
			return c.call();
		}
		<I, O> O apply(Function<I, O> f, I in) {
			return f.apply(in);
		}

		@Override
		public void close() {
			taskIdStack = origStack;
		}
	}

	<T> Callable<T> wrapCallable(Callable<T> callable) {
		final String currentTaskId = getCurrentTaskId();
		return () -> {
			try(Scope scope = threadInstance.get().new Scope(currentTaskId)) {
				return scope.apply(callable);
			}
		};
	}

	<T, R> Function<T, R> wrapFunction(Function<T, R> function) {
		final String currentTaskId = getCurrentTaskId();
		return (in) -> {
			try(Scope scope = threadInstance.get().new Scope(currentTaskId)) {
				return scope.apply(function, in);
			}
		};
	}

	void clearStack(){
		taskIdStack.clear();
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
		Event event;
		if (!isOngoingTask) {
			if (parentTaskId == null){
				parentTaskId = getParentIdFromStack();
			}
			event = new StartEvent(taskId, name, logParams, parentTaskId);
			taskIdStack.push(event.getTaskId());
		}
		else{
			event = new StartEvent(taskId, name, logParams, parentTaskId);
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
			try {
				logParams.text(Constants.EXCEPTION, t + "\n" + ExceptionUtils.getStackTrace(t));
			} catch (Exception e){
				logParams.text(Constants.EXCEPTION, t.getMessage());
			}

			logParams.string(Constants.EXCEPTION_NAME, t.toString());
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
		if (parentTaskId == null) {
			parentTaskId = getParentIdFromStack();
		}
		SpotEvent spotEvent = new SpotEvent(taskId, name, parentTaskId, status, logParams);
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

	private String getParentIdFromStack() {
		String parentId = null;
		if (!taskIdStack.isEmpty()) {
			parentId = taskIdStack.peek();
		}
		return parentId;
	}

	private void addStaticParams(@NotNull LogParams logParams) {
		logParams.string(THREAD_NAME, Thread.currentThread().getName());
		logParams.string(staticParams);
	}

	private Event getCorruptedEvent(@NotNull LogParams logParams) {
		String stackTrace = getStackTraceString();
		addStaticParams(logParams);
		logParams.text(STACK_TRACE, stackTrace);
		return createSpotEvent(null, Constants.LOG_WITHOUT_CONTEXT, logParams, TaskStatus.CORRUPTED, null, null);
	}

	static String getStackTraceString() {
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

}
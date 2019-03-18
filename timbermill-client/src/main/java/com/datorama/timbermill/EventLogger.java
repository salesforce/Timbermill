package com.datorama.timbermill;

import com.datorama.timbermill.common.Constants;
import com.datorama.timbermill.pipe.BlackHolePipe;
import com.datorama.timbermill.pipe.BufferingOutputPipe;
import com.datorama.timbermill.pipe.EventOutputPipe;
import com.datorama.timbermill.pipe.StatisticsCollectorOutputPipe;
import com.datorama.timbermill.unit.Event;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.function.Function;

import static com.datorama.timbermill.common.Constants.EXCEPTION;
import static com.datorama.timbermill.unit.Event.EventType;

final class EventLogger {
	private static final Logger LOG = LoggerFactory.getLogger(EventLogger.class);

	/**
	 * Factory related fields
	 */
	private static ImmutableMap<String, Object> staticParams;
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

	static void bootstrap(Map<String, Object> staticParameters, EventOutputPipe eventOutputPipe, boolean doHeartbeat) {
		if (isBootstrapped) {
			LOG.warn("EventLogger is already bootstrapped, ignoring this bootstrap invocation. EventOutputPipe={}, ({})", eventOutputPipe, staticParameters);
		} else {
			LOG.info("Bootstrapping EventLogger with EventOutputPipe={}, ({})", eventOutputPipe, staticParameters);
			isBootstrapped = true;
			BufferingOutputPipe bufferingOutputPipe = new BufferingOutputPipe(eventOutputPipe);
			bufferingOutputPipe.start();

			StatisticsCollectorOutputPipe statsCollector = new StatisticsCollectorOutputPipe(bufferingOutputPipe);

			if (doHeartbeat) {
				ClientHeartbeater heartbeater = new ClientHeartbeater(statsCollector, bufferingOutputPipe);
				heartbeater.start();
			}
			staticParams = new Builder<String, Object>().putAll(staticParameters).build();

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

	String spotEvent(String name, Map<String, ?> strings, Map<String, String> texts, Map<String, ?> globals, Map<String, Number> metrics) {
		Event event = createSpotEvent(name, strings, texts, globals, metrics);
		return submitEvent(event);
	}

	String getCurrentTaskId() {
		return taskIdStack.empty() ? null : taskIdStack.peek();
	}

	String logParams(LogParams logParams) {
		Event event = createInfoEvent(logParams.getStrings(), logParams.getTexts(), logParams.getGlobals(), logParams.getMetrics());
		return submitEvent(event);
	}

	String successEvent(DateTime time) {
		Event event = createEndEvent(EventType.END_SUCCESS, time, null);
		return submitEvent(event);
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

	String startEvent(String name, Map<String, ?> strings,  Map<String, String> texts,  Map<String, ?> globals,  Map<String, Number> metrics) {


		Map<String, String> allGlobals = getAllGlobals(globals);
		allGlobals.put("threadName", Thread.currentThread().getName());

		Event event = createEvent(null, name, EventType.START, new DateTime(), convertValuesToStringValues(strings), texts, allGlobals, metrics);
		return submitEvent(event);
	}

	private Map<String, String> getAllGlobals(Map<String, ?> strings) {
		Map<String, String> retStrings = convertValuesToStringValues(staticParams);
		if (strings == null){
			return retStrings;
		}
		else {
			for (Entry<String, ?> entry : strings.entrySet()) {
				retStrings.put(entry.getKey(), String.valueOf(entry.getValue()));
			}
			return retStrings;
		}
	}

	String endWithError(Throwable t, DateTime time) {
		Map<String, String> texts = new HashMap<>();
		texts.put(EXCEPTION, t + "\n" + ExceptionUtils.getStackTrace(t));
		Event event = createEndEvent(EventType.END_ERROR, time, texts);
		return submitEvent(event);
	}

    private static Map<String, String> convertValuesToStringValues(Map<String, ?> map) {
		Map<String, String> returnedMap = new HashMap<>();
		if (map != null) {
			for (Entry<String, ?> entry : map.entrySet()) {
				returnedMap.put(entry.getKey(), String.valueOf(entry.getValue()));
			}
		}
		return returnedMap;
	}

	private Event createEvent(String taskId, String name, EventType eventType, DateTime time, Map<String, String> strings,
							  Map<String, String> texts, Map<String, ?> globals, Map<String, Number> metrics) {
		if (taskId == null) {
			taskId = generateTaskId(name, time);
		}
		Event e = new Event(taskId, eventType, time, name);
		e.setStrings(strings);
		e.setTexts(texts);
		e.setGlobals(globals);
		e.setMetrics(metrics);
		StringUtils.isEmpty(null);
		if (taskIdStack.isEmpty()) {
			e.setPrimaryId(taskId);
		} else {
			e.setPrimaryId(taskIdStack.firstElement());
			e.setParentId(taskIdStack.peek());
		}
		if (eventType == EventType.START) {
			taskIdStack.push(taskId);
		}
		return e;
	}

	private String submitEvent(Event event) {
		eventOutputPipe.send(event);
		return event.getTaskId();
	}

	private static String generateTaskId(String name, DateTime time) {
		return name + '_' + time.getMillis() + '_' + Math.abs(new Random().nextInt());
	}

	private Event createInfoEvent(Map<String, ?> strings, Map<String, ?> texts, Map<String, ?> globals, Map<String, Number> metrics) {
		Event event;
		Map<String, String> newTexts = convertValuesToStringValues(texts);
		if (taskIdStack.empty()) {
			String stackTrace = getStackTraceString();
			newTexts.put("stackTrace", stackTrace);
			event = createSpotEvent(Constants.LOG_WITHOUT_CONTEXT, strings, newTexts, globals, metrics);
		} else {
			event = createEvent(taskIdStack.peek(), null, EventType.INFO, new DateTime(), convertValuesToStringValues(strings), newTexts, globals, metrics);
		}
		return event;
	}

	private Event createSpotEvent(String name, Map<String, ?> strings, Map<String, ?> texts, Map<String, ?> globals, Map<String, Number> metrics) {
		Map<String, String> convertedStringsMap = convertValuesToStringValues(strings);
		Map<String, String> convertedTextsMap = convertValuesToStringValues(texts);
		Map<String, String> convertedGlobalsMap = convertValuesToStringValues(globals);
		convertedStringsMap.putAll(convertValuesToStringValues(staticParams));
		String threadName = Thread.currentThread().getName();
		ImmutableMap<String, String> newGlobals;
		if (convertedGlobalsMap != null) {
			newGlobals = new Builder<String, String>().putAll(convertedGlobalsMap).put("threadName", threadName).build();
		} else {
			newGlobals = ImmutableMap.of("threadName", threadName);
		}
		return createEvent(null, name, EventType.SPOT, new DateTime(), convertedStringsMap, convertedTextsMap, newGlobals, metrics);
	}

	private Event createEndEvent(EventType eventType, DateTime time, Map<String, String> texts) {
		if (texts == null){
			texts = new HashMap<>();
		}
		Event e;
		if (taskIdStack.empty()) {
			String stackTrace = getStackTraceString();
			texts.put("stackTrace", stackTrace);
			e = createSpotEvent(Constants.END_WITHOUT_START, null, texts, convertValuesToStringValues(staticParams), null);
		} else {
			e = new Event(taskIdStack.pop(), eventType, time);
			e.setTexts(texts);
			if (!taskIdStack.isEmpty()) {
				e.setPrimaryId(taskIdStack.firstElement());
				e.setParentId(taskIdStack.peek());
			}
		}
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

	String startEvent(String name) {
		return startEvent(name, Maps.newHashMap(), Maps.newHashMap(), Maps.newHashMap(), Maps.newHashMap());
	}
}
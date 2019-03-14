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

	String startEvent(String taskType) {
		return startEvent(taskType, null, null, null);
	}

	String spotEvent(String taskType, Map<String, ?> attributes, Map<String, Number> metrics, Map<String, String> data) {
		Event event = createSpotEvent(taskType, attributes, metrics, data);
		return submitEvent(event);
	}

	String getCurrentTaskId() {
		return taskIdStack.empty() ? null : taskIdStack.peek();
	}

	String logAttributes(Map<String, ?> params) {
		Event event = createInfoEvent(params, null, null);
		return submitEvent(event);
	}
	String logMetrics(Map<String, Number> params) {
		Event event = createInfoEvent(null, params, null);
		return submitEvent(event);
	}

	String logData(Map<String, ?> params) {
		Event event = createInfoEvent(null, null, params);
		return submitEvent(event);
	}

	String logParams(LogParams logParams) {
		Event event = createInfoEvent(logParams.getAttributes(), logParams.getMetrics(), logParams.getData());
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

	String startEvent(String taskType, Map<String, ?> attributes, Map<String, Number> metrics, Map<String, String> data) {

		if(data == null){
			data = Maps.newHashMap();
		}
		if(metrics == null){
			metrics = Maps.newHashMap();
		}

		String threadName = Thread.currentThread().getName();
		data.put("threadName", threadName);

		Map<String, String> allAttributes = getAllAttributes(attributes);

		Event event = createEvent(null, taskType, EventType.START, new DateTime(), allAttributes, metrics, data);
		return submitEvent(event);
	}

	private Map<String, String> getAllAttributes(Map<String, ?> attributes) {
		Map<String, String> retAttrs = convertValuesToStringValues(staticParams);
		if (attributes == null){
			return retAttrs;
		}
		else {
			for (Entry<String, ?> entry : attributes.entrySet()) {
				retAttrs.put(entry.getKey(), String.valueOf(entry.getValue()));
			}
			return retAttrs;
		}
	}

	String endWithError(Throwable t, DateTime time) {
		Map<String, String> data = new HashMap<>();
		data.put(EXCEPTION, t + "\n" + ExceptionUtils.getStackTrace(t));
		Event event = createEndEvent(EventType.END_ERROR, time, data);
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

	private Event createEvent(String taskId, String taskType, EventType eventType, DateTime time, Map<String, String> attributes,
							  Map<String, Number> metrics, Map<String, String> data) {
		if (taskId == null) {
			taskId = generateTaskId(taskType, time);
		}
		Event e = new Event(taskId, eventType, time, taskType);
		e.setAttributes(attributes);
		e.setMetrics(metrics);
		e.setData(data);
		StringUtils.isEmpty(null);
		if (taskIdStack.isEmpty()) {
			e.setPrimaryTaskId(taskId);
		} else {
			e.setPrimaryTaskId(taskIdStack.firstElement());
			e.setParentTaskId(taskIdStack.peek());
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

	private static String generateTaskId(String taskType, DateTime time) {
		return taskType + '_' + time.getMillis() + '_' + Math.abs(new Random().nextInt());
	}

	private Event createInfoEvent(Map<String, ?> attributes, Map<String, Number> metrics, Map<String, ?> data) {
		Event event;
		Map<String, String> newData = convertValuesToStringValues(data);
		if (taskIdStack.empty()) {
			String stackTrace = getStackTraceString();
			newData.put("stackTrace", stackTrace);
			event = createSpotEvent(Constants.LOG_WITHOUT_CONTEXT, attributes, metrics, newData);
		} else {
			event = createEvent(taskIdStack.peek(), null, EventType.INFO, new DateTime(), convertValuesToStringValues(attributes), metrics, newData);
		}
		return event;
	}

	private Event createSpotEvent(String taskType, Map<String, ?> attributes, Map<String, Number> metrics, Map<String, ?> data) {
		Map<String, String> convertedAttributesMap = convertValuesToStringValues(attributes);
		Map<String, String> convertedDataMap = convertValuesToStringValues(data);
		convertedAttributesMap.putAll(convertValuesToStringValues(staticParams));
		String threadName = Thread.currentThread().getName();
		ImmutableMap<String, String> datas;
		if (data != null) {
			datas = new Builder<String, String>().putAll(convertedDataMap).put("threadName", threadName).build();
		} else {
			datas = ImmutableMap.of("threadName", threadName);
		}
		return createEvent(null, taskType, EventType.SPOT, new DateTime(), convertedAttributesMap, metrics, datas);
	}

	private Event createEndEvent(EventType eventType, DateTime time, Map<String, String> data) {
		if (data == null){
			data = new HashMap<>();
		}
		Event e;
		if (taskIdStack.empty()) {
			String stackTrace = getStackTraceString();
			data.put("stackTrace", stackTrace);
			e = createEvent(null, Constants.END_WITHOUT_START, EventType.SPOT, new DateTime(), convertValuesToStringValues(staticParams), null, data);
		} else {
			e = new Event(taskIdStack.pop(), eventType, time);
			e.setData(data);
			if (!taskIdStack.isEmpty()) {
				e.setPrimaryTaskId(taskIdStack.firstElement());
				e.setParentTaskId(taskIdStack.peek());
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
}
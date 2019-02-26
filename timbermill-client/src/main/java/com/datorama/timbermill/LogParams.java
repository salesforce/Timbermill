package com.datorama.timbermill;

import com.google.common.collect.Maps;

import java.util.Map;

public class LogParams {

	private Map<String, Object> attributes = Maps.newHashMap();
	private Map<String, String> data = Maps.newHashMap();
	private Map<String, Number> metrics = Maps.newHashMap();

	public static LogParams create() {
		return new LogParams();
	}

	public LogParams attr(String key, Object value) {
		attributes.put(key, value);
		return this;
	}

	public LogParams data(String key, String value) {
		data.put(key, value);
		return this;
	}

	public LogParams metric(String key, Number value) {
		metrics.put(key, value);
		return this;
	}

	public Map<String, Object> getAttributes() {
		return attributes;
	}

	public Map<String, Number> getMetrics() {
		return metrics;
	}

	public Map<String, String> getData() {
		return data;
	}
}

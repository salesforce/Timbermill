package com.datorama.timbermill;

import com.google.common.collect.Maps;

import java.util.Map;

public class LogParams {

	private Map<String, Object> strings = Maps.newHashMap();
	private Map<String, String> texts = Maps.newHashMap();
	private Map<String, Number> metrics = Maps.newHashMap();
	private Map<String, Object> globals = Maps.newHashMap();

	public static LogParams create() {
		return new LogParams();
	}

	public LogParams string(String key, Object value) {
		strings.put(key, value);
		return this;
	}

	public LogParams text(String key, String value) {
		texts.put(key, value);
		return this;
	}

	public LogParams metric(String key, Number value) {
		metrics.put(key, value);
		return this;
	}

	public LogParams global(String key, Object value) {
		globals.put(key, value);
		return this;
	}


	Map<String, Object> getStrings() {
		return strings;
	}

	Map<String, Number> getMetrics() {
		return metrics;
	}

	Map<String, String> getTexts() {
		return texts;
	}

    Map<String, Object> getGlobals() {
        return globals;
    }

}

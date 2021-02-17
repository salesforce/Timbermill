package com.datorama.oss.timbermill.unit;

import java.util.HashMap;
import java.util.Map;

public class LogParams {

	private Map<String, String> strings = new HashMap<>();
	private Map<String, String> texts = new HashMap<>();
	private Map<String, Number> metrics = new HashMap<>();
	private Map<String, String> context = new HashMap<>();

	public static LogParams create() {
		return new LogParams();
	}

	public LogParams() {
	}

	public LogParams string(String key, Object value) {
		if (key != null) {
			strings.put(key, String.valueOf(value));
		}
		return this;
	}

    public LogParams string(Map<String, String> map) {
		map.forEach((key, value) -> {
			if (key != null) {
				strings.put(key, value);
			}
		});
		return this;
    }

	public LogParams text(String key, String value) {
		if (key != null) {
			texts.put(key, value);
		}
		return this;
	}

    public LogParams text(Map<String, String> map) {
		map.forEach((key, value) -> {
			if (key != null) {
				texts.put(key, value);
			}
		});
		return this;
    }

	public LogParams metric(String key, Number value) {
		if (key != null) {
			metrics.put(key, value);
		}
		return this;
	}

    public LogParams metric(Map<String, Number> map) {
		map.forEach((key, value) -> {
			if (key != null) {
				metrics.put(key, value);
			}
		});
        return this;
    }

	public LogParams context(String key, Object value) {
		if (key != null) {
			context.put(key, String.valueOf(value));
		}
		return this;
	}

    public LogParams context(Map<String, String> map) {
        map.forEach((key, value) -> {
			if (key != null) {
				context.put(key, value);
			}
		});
        return this;
    }

    public Map<String, String> getStrings() {
		return strings;
	}

	public Map<String, Number> getMetrics() {
		return metrics;
	}

	public Map<String, String> getTexts() {
		return texts;
	}

    public Map<String, String> getContext() {
        return context;
    }

}

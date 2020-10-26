package com.datorama.oss.timbermill.unit;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogParams {

	private Map<String, String> strings = new HashMap<>();
	private Map<String, String> texts = new HashMap<>();
	private Map<String, Number> metrics = new HashMap<>();
	private Map<String, String> context = new HashMap<>();

	private List<String> logs = new ArrayList<>();

	public static LogParams create() {
		return new LogParams();
	}

	public LogParams() {
	}

	public LogParams logInfo(String s) {
		addToLogs(s, "INFO");
		return this;
	}

	public LogParams logWarn(String s) {
		addToLogs(s, "WARN");
		return this;
	}

	public LogParams logError(String s) {
		addToLogs(s, "Error");
		return this;
	}

	private void addToLogs(String log, String severity) {
		String date = ZonedDateTime.now().format(DateTimeFormatter.ofLocalizedDateTime(FormatStyle.MEDIUM));
		logs.add(String.format("[%s] [%s] - %s", date, severity, log));
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

	public List<String> getLogs() {
		return logs;
	}
}

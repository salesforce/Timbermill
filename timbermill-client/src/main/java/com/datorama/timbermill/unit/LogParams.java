package com.datorama.timbermill.unit;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.List;
import java.util.Map;

public class LogParams {

	private Map<String, String> strings = Maps.newHashMap();
	private Map<String, String> texts = Maps.newHashMap();
	private Map<String, Number> metrics = Maps.newHashMap();
	private Map<String, String> context = Maps.newHashMap();

	private List<String> logs = Lists.newArrayList();

	public static LogParams create() {
		return new LogParams();
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
		strings.put(key, String.valueOf(value));
		return this;
	}

    public LogParams string(Map<String, String> map) {
        strings.putAll(map);
        return this;
    }

	public LogParams text(String key, String value) {
		texts.put(key, value);
		return this;
	}

    public LogParams text(Map<String, String> map) {
        texts.putAll(map);
        return this;
    }

	public LogParams metric(String key, Number value) {
		metrics.put(key, value);
		return this;
	}

    public LogParams metric(Map<String, Number> map) {
        metrics.putAll(map);
        return this;
    }

	public LogParams context(String key, Object value) {
		context.put(key, String.valueOf(value));
		return this;
	}

    public LogParams context(Map<String, String> staticParams) {
        context.putAll(staticParams);
        return this;
    }

    Map<String, String> getStrings() {
		return strings;
	}

	Map<String, Number> getMetrics() {
		return metrics;
	}

	Map<String, String> getTexts() {
		return texts;
	}

    Map<String, String> getContext() {
        return context;
    }

	List<String> getLogs() {
		return logs;
	}
}

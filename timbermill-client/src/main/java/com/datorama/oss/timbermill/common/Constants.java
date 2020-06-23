package com.datorama.oss.timbermill.common;

import java.time.ZonedDateTime;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public final class Constants {
	public static final Gson GSON = new GsonBuilder().registerTypeAdapter(ZonedDateTime.class, new ZonedDateTimeConverter()).create();

	public static final String DEFAULT_ELASTICSEARCH_URL = "http://localhost:9200";
	public static final String DEFAULT_TIMBERMILL_URL = "http://localhost:8484";
	public static final String LOG_WITHOUT_CONTEXT = "LogWithoutContext";
	public static final String HEARTBEAT_TASK = "metadata_timbermill_client_heartbeat";
	public static final String EXCEPTION = "exception";
	public static final String TYPE = "_doc";
	public static final String TIMBERMILL_SCRIPT = "timbermill-script";
	public static final String ALREADY_CLOSED = "ALREADY_CLOSED";
	public static final String CORRUPTED_REASON = "corruptedReason";
	public static final int MAX_CHARS_ALLOWED_FOR_NON_ANALYZED_FIELDS = 16384;
	public static final int MAX_CHARS_ALLOWED_FOR_ANALYZED_FIELDS = 900000;
	public static final String TEXT = "text";
	public static final String STRING = "string";
	public static final String CTX = "ctx";
}


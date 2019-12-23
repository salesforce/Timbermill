package com.datorama.timbermill.common;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.time.ZonedDateTime;

public final class Constants {
	public static final Gson GSON = new GsonBuilder().registerTypeAdapter(ZonedDateTime.class, new ZonedDateTimeConverter()).create();

	public static final String LOG_WITHOUT_CONTEXT = "LogWithoutContext";
	public static final String HEARTBEAT_TASK = "metadata_timbermill_client_heartbeat";
	public static final String EXCEPTION = "exception";
	public static final String TIMBERMILL_INDEX_PREFIX = "timbermill";
	public static final String INDEX_DELIMITER = "-";
	public static final String TYPE = "_doc";
}


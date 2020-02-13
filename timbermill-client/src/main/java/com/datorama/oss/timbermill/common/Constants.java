package com.datorama.oss.timbermill.common;

import java.time.ZonedDateTime;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public final class Constants {

	public static final String HEARTBEAT_TASK = "metadata_timbermill_client_heartbeat";
	public static final String EXCEPTION = "exception";
	public static final String ALREADY_CLOSED = "ALREADY_CLOSED";
	public static final String CORRUPTED_REASON = "corruptedReason";
	public static final Gson GSON = new GsonBuilder().registerTypeAdapter(ZonedDateTime.class, new ZonedDateTimeConverter()).create();
	public static final String TIMBERMILL_SCRIPT = "timbermill-script";
	public static final String TYPE = "_doc";
}


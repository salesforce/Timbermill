package com.datorama.oss.timbermill.common;

import com.google.gson.*;

import java.lang.reflect.Type;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class ZonedDateTimeConverter implements JsonSerializer<ZonedDateTime>, JsonDeserializer<ZonedDateTime> {
	@Override
	public JsonElement serialize(ZonedDateTime t, Type type, JsonSerializationContext jsonSerializationContext) {
		return new JsonPrimitive(t.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
	}

	@Override
	public ZonedDateTime deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) {
		return ZonedDateTime.parse(jsonElement.getAsString(), DateTimeFormatter.ISO_OFFSET_DATE_TIME);
	}
}

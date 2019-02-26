package com.datorama.timbermill;

import java.lang.reflect.Type;
import org.joda.time.DateTime;
import com.google.gson.*;

public class DateTimeTypeConverter implements JsonSerializer<DateTime>, JsonDeserializer<DateTime> {
	@Override
	public JsonElement serialize(DateTime t, Type type, JsonSerializationContext jsonSerializationContext) {
		return new JsonPrimitive(t.toString());
	}

	@Override
	public DateTime deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) {
		return new DateTime(jsonElement.getAsString());
	}
}

package com.datorama.timbermill.common;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class ZonedDateTimeJacksonSerializer extends JsonSerializer<ZonedDateTime> {

	@Override
	public void serialize(ZonedDateTime zonedDateTime, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
		jsonGenerator.writeString(zonedDateTime != null ? zonedDateTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) : null);
	}
}

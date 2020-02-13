package com.datorama.oss.timbermill.unit;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
public class EventsWrapper {
    private List<Event> events;

    public EventsWrapper() {
    }

    public EventsWrapper(List<Event> events) {

        this.events = events;
    }

    public List<Event> getEvents() {
        return events;
    }

    public void setEvents(List<Event> events) {
        this.events = events;
    }

    public long estimatedSize() {
        long i = 37;
        for (Event event : events) {
            ObjectMapper om = new ObjectMapper();
            ObjectWriter ow = om.writerFor(om.getTypeFactory().constructType(Event.class));
            i += event.estimatedSize() + 1;
        }
        return i;
    }
}

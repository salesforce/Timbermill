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

    public int estimatedSize() {
        int i = 37;
        for (Event event : events) {
            i += event.estimatedSize() + 1;
        }
        return i;
    }
}

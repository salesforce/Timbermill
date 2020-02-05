package com.datorama.timbermill.unit;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.util.List;

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
            try {
                ObjectMapper om = new ObjectMapper();
                ObjectWriter ow = om.writerFor(om.getTypeFactory().constructType(Event.class));
                String eventsWrapperString = ow.writeValueAsString(event);
                i += event.estimatedSize() + 1;
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
        return i;
    }
}

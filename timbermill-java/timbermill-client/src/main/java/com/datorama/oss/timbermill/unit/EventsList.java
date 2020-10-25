package com.datorama.oss.timbermill.unit;

import java.util.LinkedList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class EventsList extends LinkedList<Event> {

    public EventsList(List<Event> events) {
        super(events);
    }

    public EventsList() {
    }
}

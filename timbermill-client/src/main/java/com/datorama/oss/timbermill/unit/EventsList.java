package com.datorama.oss.timbermill.unit;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class EventsList extends ArrayList<Event> {

    public EventsList(List<Event> events) {
        super(events);
    }

    public EventsList() {
    }
}

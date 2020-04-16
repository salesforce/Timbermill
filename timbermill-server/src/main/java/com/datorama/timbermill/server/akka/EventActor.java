package com.datorama.timbermill.server.akka;

import java.util.Collection;

import com.datorama.oss.timbermill.unit.Event;
import com.datorama.oss.timbermill.unit.EventsWrapper;
import com.datorama.timbermill.server.service.TimbermillService;

import akka.actor.AbstractActor;
import akka.japi.pf.ReceiveBuilder;

public class EventActor extends AbstractActor {

    private final TimbermillService timbermillService;

    public EventActor(TimbermillService timbermillService) {
        this.timbermillService = timbermillService;
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
                .match(EventsWrapper.class, eventsWrapper -> {
                    Collection<Event> events = eventsWrapper.getEvents();
                    timbermillService.handleEvent(events);
                })
                .build();
    }
}
package com.datorama.timbermill.server.akka;

import java.util.Collection;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.datorama.timbermill.server.service.TimbermillService;
import com.datorama.timbermill.unit.Event;
import com.datorama.timbermill.unit.EventsWrapper;

import akka.actor.AbstractActor;
import akka.japi.pf.ReceiveBuilder;

@Component
@Scope("prototype")
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
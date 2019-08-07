package com.datorama.timbermill.server.akka;

import akka.actor.AbstractActor;
import akka.japi.pf.ReceiveBuilder;
import com.datorama.timbermill.server.service.TimbermillService;
import com.datorama.timbermill.unit.Event;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

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
                .match(Event.class, event -> {
                    timbermillService.handleEvent(event);
                    getSender().tell("Event handled", self());
                })
                .build();
    }
}
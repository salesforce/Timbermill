package com.datorama.timbermill.server.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.http.javadsl.marshallers.jackson.Jackson;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;

import com.datorama.oss.timbermill.unit.EventsWrapper;
import com.datorama.timbermill.server.SpringExtension;
import org.springframework.stereotype.Component;

import static akka.actor.ActorRef.noSender;

@Component
public class Router extends AllDirectives {

    private final ActorRef eventActor;

    public Router(ActorSystem actorSystem, SpringExtension springExtension) {
        eventActor = actorSystem.actorOf(springExtension.props("eventActor"));
    }

    public Route createRoute() {
        return concat(
                post(() ->
                        path("events", () ->
                                entity(Jackson.unmarshaller(EventsWrapper.class), eventsWrapper -> {
                                    eventActor.tell(eventsWrapper, noSender());
                                    return complete("Event handled");
                                })))
        );
    }
}
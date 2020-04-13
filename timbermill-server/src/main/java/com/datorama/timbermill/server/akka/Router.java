package com.datorama.timbermill.server.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.http.javadsl.marshallers.jackson.Jackson;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;
import akka.routing.RoundRobinPool;

import com.datorama.oss.timbermill.unit.EventsWrapper;
import com.datorama.timbermill.server.service.TimbermillService;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static akka.actor.ActorRef.noSender;

@Component
public class Router extends AllDirectives {

    private final ActorRef actorPool;

    public Router(ActorSystem actorSystem, TimbermillService timbermillService, @Value("${AKKA_THREADPOOL_SIZE:10}") int akkaThreadPoolSize) {
        RoundRobinPool roundRobinPool = new RoundRobinPool(akkaThreadPoolSize);
        Props props = roundRobinPool.props(Props.create(EventActor.class, timbermillService));
        actorPool = actorSystem.actorOf(props);
    }

    public Route createRoute() {
        return concat(
                post(() ->
                        path("events", () ->
                                entity(Jackson.unmarshaller(EventsWrapper.class), eventsWrapper -> {
                                    actorPool.tell(eventsWrapper, noSender());
                                    return complete("Event handled");
                                })))
        );
    }
}
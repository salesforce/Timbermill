package com.datorama.timbermill.server;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import com.datorama.timbermill.server.akka.Router;
import com.datorama.timbermill.server.service.TimbermillService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.concurrent.CompletionStage;

@SpringBootApplication
public class TimbermillApplication {
    private static final Logger log = LoggerFactory.getLogger(TimbermillApplication.class);
    private static final int PORT = 8484;
    private static CompletionStage<ServerBinding> binding;
    private static ActorSystem system;
    private static ConfigurableApplicationContext context;

    public static void main(String[] args) {
        context = SpringApplication.run(TimbermillApplication.class, args);
        system = context.getBean(ActorSystem.class);

        final Http http = Http.get(system);
        final ActorMaterializer materializer = ActorMaterializer.create(system);

        Router router = context.getBean(Router.class);
        final Flow<HttpRequest, HttpResponse, NotUsed> flow = router.createRoute().flow(system, materializer);
        binding = http
                .bindAndHandle(flow, ConnectHttp.toHost("0.0.0.0", PORT), materializer);

        log.info("Server online at http://localhost:" + PORT + "/");
    }

    @PreDestroy
    public void tearDown(){
        binding
                .thenCompose(unbound -> unbound.terminate(Duration.ofSeconds(10)))
                .thenAccept(unbound -> system.terminate());
        TimbermillService timbermillService = context.getBean(TimbermillService.class);
        timbermillService.tearDown();

    }


}
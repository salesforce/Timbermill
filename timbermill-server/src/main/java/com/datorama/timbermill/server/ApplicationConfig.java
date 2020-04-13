package com.datorama.timbermill.server;

import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorSystem;

@Configuration
public class ApplicationConfig {

    @Value("${AKKA_SERVER_BACKLOG:10000}")
    private String serverBackLog;

    @Value("${AKKA_CONNECTION_POOL_MAX_CONNECTIONS:400}")
    private String connectionPoolMaxConnections;

    @Value("${AKKA_CONNECTION_POOL_MAX_OPEN_REQUESTS:2048}")
    private String connectionPoolMaxOpenRequests;

    @Bean
    public ActorSystem actorSystem() {
        Map<String, String> configMap = Maps.newHashMap();
        configMap.put("akka.http.server.backlog", serverBackLog);
        configMap.put("akka.http.host-connection-pool.max-connections", connectionPoolMaxConnections);
        configMap.put("akka.http.host-connection-pool.max-open-requests", connectionPoolMaxOpenRequests);

        Config akkaConfig = ConfigFactory.parseMap(configMap);
        final ActorSystem system = ActorSystem.create("default", akkaConfig);
        return system;
    }
}
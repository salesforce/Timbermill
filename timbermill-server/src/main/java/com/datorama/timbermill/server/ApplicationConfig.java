package com.datorama.timbermill.server;

import akka.actor.*;
import com.typesafe.config.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.context.*;
import org.springframework.context.annotation.*;

@Configuration
public class ApplicationConfig {

    private final ApplicationContext applicationContext;
    private final SpringExtension springAkkaExtension;

    @Autowired
    public ApplicationConfig(ApplicationContext applicationContext, SpringExtension springAkkaExtension) {
        this.applicationContext = applicationContext;
        this.springAkkaExtension = springAkkaExtension;
    }

    @Bean
    public ActorSystem actorSystem() {
        final ActorSystem system = ActorSystem.create("default", akkaConfiguration());
        springAkkaExtension.setApplicationContext(applicationContext);
        return system;
    }

    @Bean
    public Config akkaConfiguration() {
        return ConfigFactory.load();
    }
}
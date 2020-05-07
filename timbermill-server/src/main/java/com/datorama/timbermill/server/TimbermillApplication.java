package com.datorama.timbermill.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.embedded.tomcat.TomcatConnectorCustomizer;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.servlet.server.ServletWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan("com.datorama")
public class TimbermillApplication {

    @Bean
    public ServletWebServerFactory tomcatServletWebServerFactory() {
        TomcatServletWebServerFactory tomcatServletWebServerFactory = new TomcatServletWebServerFactory();
        TomcatConnectorCustomizer customizer = connector -> {
            connector.setProperty("socket.appReadBufSize", "287380");
            connector.setProperty("socket.rxBufSize", "287380");
        };
        tomcatServletWebServerFactory.addConnectorCustomizers(customizer);

        return tomcatServletWebServerFactory;
    }

    public static void main(String[] args) {
        SpringApplication.run(TimbermillApplication.class, args);
    }

}
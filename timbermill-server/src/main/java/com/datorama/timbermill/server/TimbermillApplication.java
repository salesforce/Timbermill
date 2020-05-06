package com.datorama.timbermill.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan("com.datorama")
public class TimbermillApplication {

    public static void main(String[] args) {
        SpringApplication.run(TimbermillApplication.class, args);
    }

}
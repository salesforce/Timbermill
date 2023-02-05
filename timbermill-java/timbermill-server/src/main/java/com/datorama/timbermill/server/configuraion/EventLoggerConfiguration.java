package com.datorama.timbermill.server.configuraion;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EventLoggerConfiguration {
    public EventLoggerConfiguration(@Value("skip.events") String skipEvents, @Value("not.to.skip.events.regex") String regex){
        System.setProperty("skip.events", skipEvents);
        System.setProperty("not.to.skip.events.regex", regex);
    }
}

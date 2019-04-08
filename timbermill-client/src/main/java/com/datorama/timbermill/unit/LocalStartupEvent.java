package com.datorama.timbermill.unit;

import java.time.ZonedDateTime;

public class LocalStartupEvent {
    private final String name = "timbermill_local_indexer_startup";

    private final ZonedDateTime startTime;
    public LocalStartupEvent(ZonedDateTime startTime) {
        this.startTime = startTime;
    }

    public String getName() {
        return name;
    }

    public ZonedDateTime getStartTime() {
        return startTime;
    }
}

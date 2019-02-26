package com.datorama.timbermill.pipe;

import com.datorama.timbermill.Event;
import com.datorama.timbermill.EventType;
import com.datorama.timbermill.LocalOutputPipe;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

public class LocalOutputPipeTest {

    private LocalOutputPipe localOutputPipe;

    @Before
    public void setUp() {
        localOutputPipe = new LocalOutputPipe();

    }

    @Test
    public void send() {
        Event start = new Event("ID1", EventType.START, new DateTime());
        Event success = new Event("ID1", EventType.END_SUCCESS, new DateTime());
        localOutputPipe.send(start);
        localOutputPipe.send(success);
    }
}
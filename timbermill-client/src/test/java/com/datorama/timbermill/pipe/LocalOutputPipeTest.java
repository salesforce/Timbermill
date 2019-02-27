package com.datorama.timbermill.pipe;

import com.datorama.timbermill.unit.Event;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import static com.datorama.timbermill.unit.Event.*;

public class LocalOutputPipeTest {

    private LocalOutputPipe localOutputPipe;

    @Before
    public void setUp() {
        LocalOutputPipeConfig.Builder builder = new LocalOutputPipeConfig.Builder().url("http://localhost:9200");
        LocalOutputPipeConfig config = new LocalOutputPipeConfig(builder);
        localOutputPipe = new LocalOutputPipe(config);
    }

    @Test
    public void send(){
        Event start = new Event("ID1", EventType.START, new DateTime());
        Event success = new Event("ID1", EventType.END_SUCCESS, new DateTime());
        localOutputPipe.send(start);
        localOutputPipe.send(success);
    }
}
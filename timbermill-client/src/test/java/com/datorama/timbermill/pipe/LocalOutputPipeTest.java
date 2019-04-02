package com.datorama.timbermill.pipe;

import com.datorama.timbermill.unit.Event;
import com.datorama.timbermill.unit.LogParams;
import com.datorama.timbermill.unit.StartEvent;
import com.datorama.timbermill.unit.SuccessEvent;
import org.junit.Before;
import org.junit.Test;

public class LocalOutputPipeTest {

    private LocalOutputPipe localOutputPipe;

    @Before
    public void setUp() {
        LocalOutputPipeConfig.Builder builder = new LocalOutputPipeConfig.Builder().url("http://localhost:9200");
        LocalOutputPipeConfig config = new LocalOutputPipeConfig(builder);
        localOutputPipe = new LocalOutputPipe(config);
    }

    //TODO add tests
    @Test
    public void send(){
        Event start = new StartEvent("ID1", LogParams.create(), null, null);
        Event success = new SuccessEvent(start.getTaskId(), LogParams.create());
        localOutputPipe.send(start);
        localOutputPipe.send(success);
    }
}
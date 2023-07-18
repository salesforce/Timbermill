package com.datorama.timbermill.server.service;

import com.datorama.oss.timbermill.TimberLogger;
import com.datorama.oss.timbermill.annotation.TimberLogTask;
import com.datorama.oss.timbermill.common.persistence.PersistenceHandler;
import com.datorama.oss.timbermill.unit.Event;
import com.datorama.oss.timbermill.unit.LogParams;
import com.datorama.oss.timbermill.unit.StartEvent;
import junit.framework.TestCase;
import org.junit.BeforeClass;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class TimbermillServiceTest extends TestCase {

    private TimbermillService service;

    static final String EVENT = "Event";
    static final int CAPACITY = 1;


    @BeforeClass
    public void init() {
        service = Mockito.mock(TimbermillService.class, Mockito.CALLS_REAL_METHODS);

        // Initialize the fields you need for your test
        ReflectionTestUtils.setField(service, "skipEventsFlag", "true");
        ReflectionTestUtils.setField(service, "notToSkipRegex", ".*TestEvent.*");
    }

    @TimberLogTask(name = EVENT)
    public void testHandleEvents() {

        TimbermillService timbermillService = Mockito.mock(TimbermillService.class, Mockito.CALLS_REAL_METHODS);

        ReflectionTestUtils.setField(timbermillService, "skipEventsFlag", "true");
        ReflectionTestUtils.setField(timbermillService, "notToSkipRegex", "event");

        PersistenceHandler persistenceHandler = Mockito.mock(PersistenceHandler.class);
        ReflectionTestUtils.setField(timbermillService, "persistenceHandler", persistenceHandler);

        BlockingQueue<Event> eventsQueue = new LinkedBlockingQueue<>(CAPACITY);
        ReflectionTestUtils.setField(service, "eventsQueue", eventsQueue);

        BlockingQueue<Event> overflowedQueue = new LinkedBlockingQueue<>(CAPACITY);
        ReflectionTestUtils.setField(service, "overflowedQueue", overflowedQueue);


        Event event = new StartEvent(TimberLogger.getCurrentTaskId(), EVENT, new LogParams(), null);

        service.handleEvents(Collections.singletonList(event));

        assertTrue(eventsQueue.contains(event));

    }

}
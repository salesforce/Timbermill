package com.datorama.timbermill.server.service;

import com.datorama.oss.timbermill.TimberLogger;
import com.datorama.oss.timbermill.annotation.TimberLogTask;
import com.datorama.oss.timbermill.common.persistence.PersistenceHandler;
import com.datorama.oss.timbermill.common.ratelimiter.RateLimiterUtil;
import com.datorama.oss.timbermill.unit.Event;
import com.datorama.oss.timbermill.unit.LogParams;
import com.datorama.oss.timbermill.unit.StartEvent;
import com.google.common.cache.LoadingCache;
import io.github.resilience4j.ratelimiter.RateLimiter;
import junit.framework.TestCase;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;



public class TimbermillServiceTest extends TestCase {

    private static TimbermillService service = Mockito.mock(TimbermillService.class, Mockito.CALLS_REAL_METHODS);

    static final String EVENT = "event";
    static final String KEEP = "keep_event";
    static final String SKIP = "skip_event";
    static final int CAPACITY = 10;

    static final int LIMIT_FOR_PERIOD = 30000;
    static final int LIMIT_REFRESH_PERIOD_MINUTES = 1;
    static final int RATE_LIMITER_CAPACITY = 10;

    static PersistenceHandler persistenceHandler = Mockito.mock(PersistenceHandler.class);
    static BlockingQueue<Event> eventsQueue = new LinkedBlockingQueue<>(CAPACITY);
    BlockingQueue<Event> overflowedQueue = new LinkedBlockingQueue<>(CAPACITY);
    LoadingCache<String, RateLimiter> rateLimiterMap = RateLimiterUtil.initRateLimiter(LIMIT_FOR_PERIOD, Duration.ofMinutes(LIMIT_REFRESH_PERIOD_MINUTES), RATE_LIMITER_CAPACITY);


    public static void init() {

        // Initialize the fields you need for your test
//        ReflectionTestUtils.setField(service, "skipEventsFlag", "true");
//        ReflectionTestUtils.setField(service, "notToSkipRegex", ".*TestEvent.*");
//        final TimbermillService service = Mockito.mock(TimbermillService.class, Mockito.CALLS_REAL_METHODS);
        System.out.print("Init()");

    }

    @TimberLogTask(name = EVENT)
    public void testHandleEventsOneMatchingTask() {

        ReflectionTestUtils.setField(service, "persistenceHandler", persistenceHandler);
        ReflectionTestUtils.setField(service, "eventsQueue", eventsQueue);
        ReflectionTestUtils.setField(service, "overflowedQueue", overflowedQueue);
        ReflectionTestUtils.setField(service, "rateLimiterMap", rateLimiterMap);

        ReflectionTestUtils.setField(service, "skipEventsFlag", "true");
        ReflectionTestUtils.setField(service, "notToSkipRegex", ".*keep.*");

        Event event = new StartEvent(TimberLogger.getCurrentTaskId(), KEEP, new LogParams(), null);

        service.handleEvents(Collections.singletonList(event));

        assertTrue(eventsQueue.contains(event));
    }

    @TimberLogTask(name = EVENT)
    public void testHandleEventsOneNotMatchingTask() {

        ReflectionTestUtils.setField(service, "persistenceHandler", persistenceHandler);
        ReflectionTestUtils.setField(service, "eventsQueue", eventsQueue);
        ReflectionTestUtils.setField(service, "overflowedQueue", overflowedQueue);
        ReflectionTestUtils.setField(service, "rateLimiterMap", rateLimiterMap);

        ReflectionTestUtils.setField(service, "skipEventsFlag", "true");
        ReflectionTestUtils.setField(service, "notToSkipRegex", ".*keep.*");

        Event event = new StartEvent(TimberLogger.getCurrentTaskId(), SKIP, new LogParams(), null);

        service.handleEvents(Collections.singletonList(event));

        assertFalse(eventsQueue.contains(event));
    }

    @TimberLogTask(name = EVENT)
    public void testHandleEventsMultipleTasks() {

        ReflectionTestUtils.setField(service, "persistenceHandler", persistenceHandler);
        ReflectionTestUtils.setField(service, "eventsQueue", eventsQueue);
        ReflectionTestUtils.setField(service, "overflowedQueue", overflowedQueue);
        ReflectionTestUtils.setField(service, "rateLimiterMap", rateLimiterMap);

        ReflectionTestUtils.setField(service, "skipEventsFlag", "true");
        ReflectionTestUtils.setField(service, "notToSkipRegex", ".keep.*");

        Event eventToKeep1 = new StartEvent(TimberLogger.getCurrentTaskId(), KEEP + "1", new LogParams(), null);
        Event eventToKeep2 = new StartEvent(TimberLogger.getCurrentTaskId(), KEEP + "2", new LogParams(), null);
        Event eventToSkip1 = new StartEvent(TimberLogger.getCurrentTaskId(), SKIP + "1", new LogParams(), null);
        Event eventToSkip2 = new StartEvent(TimberLogger.getCurrentTaskId(), SKIP + "1", new LogParams(), null);


        service.handleEvents(Arrays.asList(eventToKeep1, eventToSkip1, eventToKeep2, eventToSkip2));

        assertTrue(eventsQueue.contains(eventToKeep1));
        assertTrue(eventsQueue.contains(eventToKeep2));
        assertFalse(eventsQueue.contains(eventToSkip1));
        assertFalse(eventsQueue.contains(eventToSkip2));
    }

    @TimberLogTask(name = EVENT)
    public void testHandleEventsMultipleTasksFlagClosed() {

        ReflectionTestUtils.setField(service, "persistenceHandler", persistenceHandler);
        ReflectionTestUtils.setField(service, "eventsQueue", eventsQueue);
        ReflectionTestUtils.setField(service, "overflowedQueue", overflowedQueue);
        ReflectionTestUtils.setField(service, "rateLimiterMap", rateLimiterMap);

        ReflectionTestUtils.setField(service, "skipEventsFlag", "false");
        ReflectionTestUtils.setField(service, "notToSkipRegex", ".keep.*");

        Event eventToKeep1 = new StartEvent(TimberLogger.getCurrentTaskId(), KEEP + "1", new LogParams(), null);
        Event eventToKeep2 = new StartEvent(TimberLogger.getCurrentTaskId(), KEEP + "2", new LogParams(), null);
        Event eventToSkip1 = new StartEvent(TimberLogger.getCurrentTaskId(), SKIP + "1", new LogParams(), null);
        Event eventToSkip2 = new StartEvent(TimberLogger.getCurrentTaskId(), SKIP + "1", new LogParams(), null);


        service.handleEvents(Arrays.asList(eventToKeep1, eventToSkip1, eventToKeep2, eventToSkip2));

        assertTrue(eventsQueue.contains(eventToKeep1));
        assertTrue(eventsQueue.contains(eventToKeep2));
        assertTrue(eventsQueue.contains(eventToSkip1));
        assertTrue(eventsQueue.contains(eventToSkip2));
    }
}
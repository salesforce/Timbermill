package com.datorama.timbermill.server.service;

import com.datorama.oss.timbermill.TimberLogger;
import com.datorama.oss.timbermill.annotation.TimberLogTask;
import com.datorama.oss.timbermill.common.persistence.PersistenceHandler;
import com.datorama.oss.timbermill.unit.Event;
import com.datorama.oss.timbermill.unit.LogParams;
import com.datorama.oss.timbermill.unit.StartEvent;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import io.github.resilience4j.ratelimiter.RateLimiter;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.util.ReflectionTestUtils;
import com.datorama.oss.timbermill.common.ratelimiter.RateLimiterUtil;

import javax.annotation.CheckForNull;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.*;
import java.time.Duration;

import static org.junit.Assert.assertTrue;


public class TimbermillServiceTest {

    private static TimbermillService service = Mockito.mock(TimbermillService.class, Mockito.CALLS_REAL_METHODS);
//    PersistenceHandler persistenceHandler = Mockito.mock(PersistenceHandler.class);
//    ReflectionTestUtils.setField(service, "persistenceHandler", persistenceHandler);
//
//    BlockingQueue<Event> eventsQueue = new LinkedBlockingQueue<>(CAPACITY);
//    ReflectionTestUtils.setField(service, "eventsQueue", eventsQueue);
//
//    BlockingQueue<Event> overflowedQueue = new LinkedBlockingQueue<>(CAPACITY);
//    ReflectionTestUtils.setField(service, "overflowedQueue", overflowedQueue);
//
//    LoadingCache<String, RateLimiter> rateLimiterMap = RateLimiterUtil.initRateLimiter(LIMIT_FOR_PERIOD, Duration.ofMinutes(LIMIT_REFRESH_PERIOD_MINUTES), RATE_LIMITER_CAPACITY);
//    ReflectionTestUtils.setField(service, "rateLimiterMap", rateLimiterMap);

    static PersistenceHandler persistenceHandler;
    static BlockingQueue<Event> eventsQueue;
    BlockingQueue<Event> overflowedQueue;
    LoadingCache<String, RateLimiter> rateLimiterMap;

    static final String EVENT = "Event";
    static final int CAPACITY = 1;

    static final int LIMIT_FOR_PERIOD = 30000;
    static final int LIMIT_REFRESH_PERIOD_MINUTES = 1;
    static final int RATE_LIMITER_CAPACITY = 10;


    @BeforeAll
    public static void init() {

        // Initialize the fields you need for your test
//        ReflectionTestUtils.setField(service, "skipEventsFlag", "true");
//        ReflectionTestUtils.setField(service, "notToSkipRegex", ".*TestEvent.*");
//        final TimbermillService service = Mockito.mock(TimbermillService.class, Mockito.CALLS_REAL_METHODS);

        PersistenceHandler persistenceHandler = Mockito.mock(PersistenceHandler.class);
        ReflectionTestUtils.setField(service, "persistenceHandler", persistenceHandler);

        BlockingQueue<Event> eventsQueue = new LinkedBlockingQueue<>(CAPACITY);
        ReflectionTestUtils.setField(service, "eventsQueue", eventsQueue);

        BlockingQueue<Event> overflowedQueue = new LinkedBlockingQueue<>(CAPACITY);
        ReflectionTestUtils.setField(service, "overflowedQueue", overflowedQueue);

        LoadingCache<String, RateLimiter> rateLimiterMap = RateLimiterUtil.initRateLimiter(LIMIT_FOR_PERIOD, Duration.ofMinutes(LIMIT_REFRESH_PERIOD_MINUTES), RATE_LIMITER_CAPACITY);
        ReflectionTestUtils.setField(service, "rateLimiterMap", rateLimiterMap);

    }

    @Test
//    @TimberLogTask(name = EVENT)
    public void testHandleEvents() {

        ReflectionTestUtils.setField(service, "skipEventsFlag", "true");
        ReflectionTestUtils.setField(service, "notToSkipRegex", "E.*");

        Event event = new StartEvent(TimberLogger.getCurrentTaskId(), EVENT, new LogParams(), null);

        service.handleEvents(Collections.singletonList(event));

        assertTrue(eventsQueue.contains(event));

    }

}
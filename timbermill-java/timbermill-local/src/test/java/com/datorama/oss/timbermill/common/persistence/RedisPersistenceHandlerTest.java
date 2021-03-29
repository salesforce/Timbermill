package com.datorama.oss.timbermill.common.persistence;

import com.datorama.oss.timbermill.common.redis.RedisServiceConfig;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

public class RedisPersistenceHandlerTest extends PersistenceHandlerTest{

    @BeforeClass
    public static void init()  {
        Map<String, Object> persistenceHandlerParams = new HashMap<>();
        persistenceHandlerParams.put(PersistenceHandler.MAX_FETCHED_BULKS_IN_ONE_TIME, 10);
        persistenceHandlerParams.put(PersistenceHandler.MAX_FETCHED_EVENTS_IN_ONE_TIME, 3);
        persistenceHandlerParams.put(PersistenceHandler.MAX_INSERT_TRIES, 3);
        persistenceHandlerParams.put(RedisPersistenceHandler.REDIS_CONFIG, new RedisServiceConfig("localhost", 6379, "", "", "",
                false, 86400, 100, 10, 10, 10, 3));
        PersistenceHandlerTest.init(persistenceHandlerParams, "redis");
    }

    @Test
    public void hasFailedBulks() throws InterruptedException, ExecutionException {
        super.hasFailedBulks();
    }

    @Test
    public void fetchFailedBulks() throws InterruptedException, ExecutionException {
        super.fetchFailedBulks();
    }

    @Test
    public void fetchOverflowedEvents() throws InterruptedException, ExecutionException {
        super.fetchOverflowedEvents();
    }

    @Test
    public void fetchesCounter() throws InterruptedException, ExecutionException {
        super.fetchesCounter();
    }

    @Test
    public void failedBulksAmount() throws InterruptedException, ExecutionException {
        super.failedBulksAmount();
    }

    @Test
    public void overflowedEventsListsAmount() throws InterruptedException, ExecutionException {
        super.overflowedEventsListsAmount();
    }

    @Test
    public void fetchMaximumBulksAmount() throws InterruptedException, ExecutionException {
        super.fetchMaximumBulksAmount();
    }

    @Test
    public void fetchMaximumEventsAmount() throws InterruptedException, ExecutionException {
        super.fetchMaximumEventsAmount();
    }

    @Test
    public void dropAndRecreateTable() throws InterruptedException, ExecutionException {
        super.dropAndRecreateTable();
    }

    @Test
    public void testMultiThreadSafety() throws InterruptedException, ExecutionException {
        super.testMultiThreadSafety();
    }

    @Test
    public void fetchExpiredFailedBulks() throws InterruptedException, ExecutionException {
        int amount = 15;
        for (int i = 0 ; i < amount ; i++){
            ((RedisPersistenceHandler)persistenceHandler).persistBulkRequest(Mock.createMockDbBulkRequest(), bulkNum, 0).get();
        }
        // all previous keys should be expired

        persistenceHandler.persistBulkRequest(Mock.createMockDbBulkRequest(), bulkNum).get();

        assertEquals(1, persistenceHandler.failedBulksAmount());
        assertEquals(1, persistenceHandler.fetchAndDeleteFailedBulks().size());
    }
}

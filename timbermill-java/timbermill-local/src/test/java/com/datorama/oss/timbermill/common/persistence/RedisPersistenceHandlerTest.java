package com.datorama.oss.timbermill.common.persistence;

import com.datorama.oss.timbermill.common.redis.RedisServiceConfig;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class RedisPersistenceHandlerTest extends PersistenceHandlerTest{

    @BeforeClass
    public static void init()  {
        Map<String, Object> persistenceHandlerParams = new HashMap<>();
        persistenceHandlerParams.put(PersistenceHandler.MAX_FETCHED_BULKS_IN_ONE_TIME, maxFetchedBulks);
        persistenceHandlerParams.put(PersistenceHandler.MAX_FETCHED_EVENTS_IN_ONE_TIME, 3);
        persistenceHandlerParams.put(PersistenceHandler.MAX_INSERT_TRIES, 3);
        persistenceHandlerParams.put(RedisPersistenceHandler.REDIS_CONFIG, new RedisServiceConfig("localhost", 6379, "", "", "",
                false, 604800, 100, 10, 10, 10, 3));
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
    public void fetchesCounter() throws InterruptedException, ExecutionException {
        super.fetchesCounter();
    }

    @Test
    public void failedBulksAmount() throws InterruptedException, ExecutionException {
        super.failedBulksAmount();
    }

    @Test
    public void fetchMaximumBulksAmount() throws InterruptedException, ExecutionException {
        super.fetchMaximumBulksAmount();
    }

    @Test
    public void dropAndRecreateTable() throws InterruptedException, ExecutionException {
        super.dropAndRecreateTable();
    }

    @Test
    public void testMultiThreadSafety() throws InterruptedException, ExecutionException {
        super.testMultiThreadSafety();
    }
}

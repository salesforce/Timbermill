package com.datorama.oss.timbermill.common.persistence;

import com.datorama.oss.timbermill.ElasticsearchClient;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.junit.AfterClass;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public abstract class PersistenceHandlerTest {
    protected static PersistenceHandler persistenceHandler;
    protected static int maxFetchedBulks = 10;
    protected static int bulkNum = 1;

    protected static void init(Map<String, Object> params, String persistenceHandlerStrategy) {
        persistenceHandler = PersistenceHandlerUtil.getPersistenceHandler(persistenceHandlerStrategy, params);
    }

    @Before
    public void emptyBeforeTest() {
        persistenceHandler.reset();
    }

    @AfterClass
    public static void tearDown(){
        persistenceHandler.reset();
        persistenceHandler.close();
    }

    public void hasFailedBulks() throws ExecutionException, InterruptedException {
        DbBulkRequest dbBulkRequest = PersistenceHandlerTest.MockBulkRequest.createMockDbBulkRequest();
        persistenceHandler.persistBulkRequest(dbBulkRequest, bulkNum).get();
        assertTrue(persistenceHandler.hasFailedBulks());
    }

    public void fetchFailedBulks() throws ExecutionException, InterruptedException {

        DbBulkRequest dbBulkRequest = PersistenceHandlerTest.MockBulkRequest.createMockDbBulkRequest();
        persistenceHandler.persistBulkRequest(dbBulkRequest, bulkNum).get();
        List<DbBulkRequest> fetchedRequests = persistenceHandler.fetchAndDeleteFailedBulks();
        assertEquals(1, fetchedRequests.size());

        DbBulkRequest dbBulkRequestFromDisk = fetchedRequests.get(0);
        assertEquals(getRequestAsString(dbBulkRequest), getRequestAsString(dbBulkRequestFromDisk));

        DbBulkRequest dbBulkRequest2 = PersistenceHandlerTest.MockBulkRequest.createMockDbBulkRequest();
        DbBulkRequest dbBulkRequest3 = PersistenceHandlerTest.MockBulkRequest.createMockDbBulkRequest();
        persistenceHandler.persistBulkRequest(dbBulkRequest2, bulkNum).get();
        persistenceHandler.persistBulkRequest(dbBulkRequest3, bulkNum).get();
        assertEquals(2, persistenceHandler.failedBulksAmount());
    }

    public void fetchesCounter() throws InterruptedException, ExecutionException {
        DbBulkRequest dbBulkRequest = PersistenceHandlerTest.MockBulkRequest.createMockDbBulkRequest();
        persistenceHandler.persistBulkRequest(dbBulkRequest, bulkNum).get();
        DbBulkRequest fetchedRequest = persistenceHandler.fetchAndDeleteFailedBulks().get(0);
        persistenceHandler.persistBulkRequest(fetchedRequest, bulkNum).get();
        fetchedRequest = persistenceHandler.fetchAndDeleteFailedBulks().get(0);
        assertEquals(2, fetchedRequest.getTimesFetched());
    }

    public void failedBulksAmount() throws InterruptedException, ExecutionException {
        int amount = 3;
        DbBulkRequest dbBulkRequest;
        for (int i = 0 ; i < amount ; i++){
            dbBulkRequest = PersistenceHandlerTest.MockBulkRequest.createMockDbBulkRequest();
            persistenceHandler.persistBulkRequest(dbBulkRequest, bulkNum).get();
        }
        assertEquals(3, persistenceHandler.failedBulksAmount());
        assertEquals(3, persistenceHandler.failedBulksAmount()); // to make sure the db didn't change after the call to failedBulksAmount
    }

    public void fetchMaximumBulksAmount() throws InterruptedException, ExecutionException {
        DbBulkRequest dbBulkRequest;
        int extraBulks = 2;
        for (int i = 0 ; i < maxFetchedBulks + extraBulks ; i++){
            dbBulkRequest = PersistenceHandlerTest.MockBulkRequest.createMockDbBulkRequest();
            persistenceHandler.persistBulkRequest(dbBulkRequest, bulkNum).get();
        }
        List<DbBulkRequest> fetchedRequests = persistenceHandler.fetchAndDeleteFailedBulks();
        assertEquals(maxFetchedBulks,fetchedRequests.size());
        assertEquals(extraBulks, persistenceHandler.failedBulksAmount());
    }

    public void dropAndRecreateTable() throws InterruptedException, ExecutionException {
        DbBulkRequest dbBulkRequest = PersistenceHandlerTest.MockBulkRequest.createMockDbBulkRequest();
        persistenceHandler.persistBulkRequest(dbBulkRequest, bulkNum).get();

        persistenceHandler.reset();
        assertFalse(persistenceHandler.hasFailedBulks());
    }

    public void testMultiThreadSafety() throws InterruptedException, ExecutionException {

        int numOfThreads = 15;
        AtomicBoolean isHealthCheckFailed = new AtomicBoolean(false);
        AtomicBoolean keepExecuting = new AtomicBoolean(true);
        ExecutorService executorService = Executors.newFixedThreadPool(numOfThreads);


        // insert some bulks to disk
        for (int i = 0 ; i < 10 ; i++){
            DbBulkRequest dbBulkRequest = PersistenceHandlerTest.MockBulkRequest.createMockDbBulkRequest();
            persistenceHandler.persistBulkRequest(dbBulkRequest, bulkNum).get();
        }

        Runnable fetchAndPersistTask = () -> {
            while (keepExecuting.get()) {
                try {
                    persistenceHandler.hasFailedBulks();
                } catch (Exception e) {
                    isHealthCheckFailed.set(true);
                    break;
                }
                try {
                    fetchAndPersist();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        for (int i = 0; i < numOfThreads; i++) {
            executorService.execute(fetchAndPersistTask);
        }

        Thread.sleep(2000);

        // stop threads and wait
        keepExecuting.set(false);
        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        assertFalse(isHealthCheckFailed.get());
    }

    // region Test Helpers

    private void fetchAndPersist() {
        if (persistenceHandler.hasFailedBulks()) {
            List<DbBulkRequest> failedRequestsFromDisk = persistenceHandler.fetchAndDeleteFailedBulks();
            if (failedRequestsFromDisk.size() == 0) {
                return;
            }
            for (DbBulkRequest dbBulkRequest : failedRequestsFromDisk) {
                persistenceHandler.persistBulkRequest(dbBulkRequest, bulkNum);
            }
        }
    }

    private String getRequestAsString(DbBulkRequest dbBulkRequest) {
        return dbBulkRequest.getRequest().requests().get(0).toString();
    }

    public static class MockBulkRequest {
        static UpdateRequest createMockRequest() {
            String taskId = UUID.randomUUID().toString();
            String index = "timbermill-test";
            UpdateRequest updateRequest = new UpdateRequest(index, ElasticsearchClient.TYPE, taskId);
            Script script = new Script(ScriptType.STORED, null, ElasticsearchClient.TIMBERMILL_SCRIPT, new HashMap<>());
            updateRequest.script(script);
            return updateRequest;
        }

        static DbBulkRequest createMockDbBulkRequest() {
            BulkRequest bulkRequest = new BulkRequest();
            for (int i = 0 ; i < 3 ; i++){
                bulkRequest.add(createMockRequest());
            }
            return new DbBulkRequest(bulkRequest);
        }
    }

    // endregion

}



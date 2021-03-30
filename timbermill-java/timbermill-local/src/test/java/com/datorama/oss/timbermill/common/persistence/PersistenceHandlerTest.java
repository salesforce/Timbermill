package com.datorama.oss.timbermill.common.persistence;

import com.datorama.oss.timbermill.ElasticsearchClient;
import com.datorama.oss.timbermill.unit.*;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.junit.AfterClass;
import org.junit.Before;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public abstract class PersistenceHandlerTest {
    protected static PersistenceHandler persistenceHandler;
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
        DbBulkRequest dbBulkRequest = Mock.createMockDbBulkRequest();
        persistenceHandler.persistBulkRequest(dbBulkRequest, bulkNum).get();
        assertTrue(persistenceHandler.hasFailedBulks());
    }

    public void fetchFailedBulks() throws ExecutionException, InterruptedException {

        DbBulkRequest dbBulkRequest = Mock.createMockDbBulkRequest();
        persistenceHandler.persistBulkRequest(dbBulkRequest, bulkNum).get();
        List<DbBulkRequest> fetchedRequests = persistenceHandler.fetchAndDeleteFailedBulks();
        assertEquals(1, fetchedRequests.size());

        DbBulkRequest dbBulkRequestFromDisk = fetchedRequests.get(0);
        assertEquals(getRequestAsString(dbBulkRequest), getRequestAsString(dbBulkRequestFromDisk));

        DbBulkRequest dbBulkRequest2 = Mock.createMockDbBulkRequest();
        DbBulkRequest dbBulkRequest3 = Mock.createMockDbBulkRequest();
        persistenceHandler.persistBulkRequest(dbBulkRequest2, bulkNum).get();
        persistenceHandler.persistBulkRequest(dbBulkRequest3, bulkNum).get();
        assertEquals(2, persistenceHandler.failedBulksAmount());
    }

    public void fetchOverflowedEvents() throws ExecutionException, InterruptedException {
        ArrayList<Event> events = Mock.createMockEventsList();

        persistenceHandler.persistEvents(events);
        List<Event> fetchedEvents = persistenceHandler.fetchAndDeleteOverflowedEvents();
        assertEquals(5, fetchedEvents.size());
        fetchedEvents = persistenceHandler.fetchAndDeleteOverflowedEvents();
        assertEquals(0, fetchedEvents.size());
    }

    public void fetchesCounter() throws InterruptedException, ExecutionException {
        DbBulkRequest dbBulkRequest = Mock.createMockDbBulkRequest();
        persistenceHandler.persistBulkRequest(dbBulkRequest, bulkNum).get();
        DbBulkRequest fetchedRequest = persistenceHandler.fetchAndDeleteFailedBulks().get(0);
        persistenceHandler.persistBulkRequest(fetchedRequest, bulkNum).get();
        fetchedRequest = persistenceHandler.fetchAndDeleteFailedBulks().get(0);
        assertEquals(2, fetchedRequest.getTimesFetched());
    }

    public void failedBulksAmount() throws InterruptedException, ExecutionException {
        int amount = 250;
        for (int i = 0 ; i < amount ; i++){
            persistenceHandler.persistBulkRequest(Mock.createMockDbBulkRequest(), bulkNum).get();
        }

        assertEquals(amount, persistenceHandler.failedBulksAmount());
        assertEquals(amount, persistenceHandler.failedBulksAmount()); // to make sure the db didn't change after the call to failedBulksAmount
    }

    public void overflowedEventsListsAmount() throws InterruptedException, ExecutionException {
        int amount = 3;
        for (int i = 0 ; i < amount ; i++){
            persistenceHandler.persistEvents(Mock.createMockEventsList());
        }

        assertEquals(amount, persistenceHandler.overFlowedEventsListsAmount());
        assertEquals(amount, persistenceHandler.overFlowedEventsListsAmount()); // to make sure the db didn't change after the call to OverflowedEventsAmount
    }

    public void fetchMaximumBulksAmount() throws InterruptedException, ExecutionException {
        int extraBulks = 30;
        int maxFetchedBulks = persistenceHandler.getMaxFetchedBulksInOneTime();
        for (int i = 0; i < maxFetchedBulks + extraBulks ; i++){
            persistenceHandler.persistBulkRequest(Mock.createMockDbBulkRequest(), bulkNum).get();
        }
        List<DbBulkRequest> fetchedRequests = persistenceHandler.fetchAndDeleteFailedBulks();
        assertEquals(maxFetchedBulks,fetchedRequests.size());
        assertEquals(extraBulks, persistenceHandler.failedBulksAmount());
    }

    public void fetchMaximumEventsAmount() throws InterruptedException, ExecutionException {
        int extraEventLists = 2;
        int maxFetchedEventsLists = persistenceHandler.getMaxFetchedEventsListsInOneTime();
        for (int i = 0; i < maxFetchedEventsLists + extraEventLists ; i++){
            persistenceHandler.persistEvents(Mock.createMockEventsList());
        }

        int mockListSize = Mock.createMockEventsList().size();
        List<Event> fetchedEvents = persistenceHandler.fetchAndDeleteOverflowedEvents();
        assertEquals(maxFetchedEventsLists * mockListSize, fetchedEvents.size());
        assertEquals(extraEventLists, persistenceHandler.overFlowedEventsListsAmount());
    }

    public void dropAndRecreateTable() throws InterruptedException, ExecutionException {
        DbBulkRequest dbBulkRequest = Mock.createMockDbBulkRequest();
        persistenceHandler.persistBulkRequest(dbBulkRequest, bulkNum).get();

        persistenceHandler.reset();
        assertFalse(persistenceHandler.hasFailedBulks());
    }

    // region Test Helpers

    private String getRequestAsString(DbBulkRequest dbBulkRequest) {
        return dbBulkRequest.getRequest().requests().get(0).toString();
    }

    public static class Mock {

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

        static ArrayList<Event> createMockEventsList() {
            return new ArrayList<>(
                    Arrays.asList(new StartEvent(), new InfoEvent(), new SuccessEvent(), new ErrorEvent(), new SpotEvent()));
        }

    }

    // endregion
}



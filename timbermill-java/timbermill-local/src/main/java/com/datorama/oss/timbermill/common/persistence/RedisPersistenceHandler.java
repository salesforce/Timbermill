package com.datorama.oss.timbermill.common.persistence;

import com.datorama.oss.timbermill.unit.Event;

import java.util.ArrayList;
import java.util.List;

public class RedisPersistenceHandler extends PersistenceHandler {
    @Override
    public List<DbBulkRequest> fetchAndDeleteFailedBulks() {
        return null;
    }

    @Override
    public List<Event> fetchAndDeleteOverflowedEvents() {
        return null;
    }

    @Override
    public void persistBulkRequest(DbBulkRequest dbBulkRequest, int bulkNum) {

    }

    @Override
    void persistEvents(ArrayList<Event> events) {

    }

    @Override
    public boolean hasFailedBulks() {
        return false;
    }

    @Override
    public boolean isCreatedSuccessfully() {
        return false;
    }

    @Override
    long failedBulksAmount() {
        return 0;
    }

    @Override
    long overFlowedEventsAmount() {
        return 0;
    }

    @Override
    public void close() {

    }
}

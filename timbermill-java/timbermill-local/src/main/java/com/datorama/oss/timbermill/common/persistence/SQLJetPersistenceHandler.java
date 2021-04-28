package com.datorama.oss.timbermill.common.persistence;

import com.datorama.oss.timbermill.common.KamonConstants;
import com.datorama.oss.timbermill.common.exceptions.MaximumInsertTriesException;
import com.datorama.oss.timbermill.unit.Event;
import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.SqlJetTransactionMode;
import org.tmatesoft.sqljet.core.table.ISqlJetCursor;
import org.tmatesoft.sqljet.core.table.ISqlJetTable;
import org.tmatesoft.sqljet.core.table.SqlJetDb;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SQLJetPersistenceHandler extends PersistenceHandler {
	static final String LOCATION_IN_DISK = "LOCATION_IN_DISK";

	private static final String DB_NAME = "timbermillJetDB26012021.db";
	private static final String FAILED_BULKS_TABLE_NAME = "failed_bulks";
	private static final String OVERFLOWED_EVENTS_TABLE_NAME = "overflowed_events";
	private static final String ID = "id";
	private static final String FAILED_TASK = "failedTask";
	private static final String OVERFLOWED_EVENT = "overflowedEvent";
	private static final String INSERT_TIME = "insertTime";
	private static final String TIMES_FETCHED = "timesFetched";
	private static final String CREATE_BULK_TABLE =
			"CREATE TABLE IF NOT EXISTS " + FAILED_BULKS_TABLE_NAME + " (" + ID + " INTEGER PRIMARY KEY AUTOINCREMENT, " + FAILED_TASK + " BLOB NOT NULL, " + INSERT_TIME + " TEXT, "
					+ TIMES_FETCHED + " INTEGER)";
	private static final String CREATE_EVENT_TABLE =
			"CREATE TABLE IF NOT EXISTS " + OVERFLOWED_EVENTS_TABLE_NAME + " (" + ID + " INTEGER PRIMARY KEY AUTOINCREMENT, " + OVERFLOWED_EVENT + " BLOB NOT NULL, " + INSERT_TIME + " TEXT)";
	private static final Logger LOG = LoggerFactory.getLogger(SQLJetPersistenceHandler.class);

	private String locationInDisk;
	private SqlJetDb db;
	private ISqlJetTable failedBulkTable;
	private ISqlJetTable overFlowedEventsTable;
	private static ExecutorService executorService = Executors.newFixedThreadPool(1);

	SQLJetPersistenceHandler(int maxFetchedBulks, int maxFetchedEvents, int maxInsertTries, String locationInDisk) {
		super(maxFetchedBulks, maxFetchedEvents, maxInsertTries);
		this.locationInDisk = locationInDisk;
		init();
	}

	private void init(){
		// initializing database
		if (!StringUtils.isEmpty(locationInDisk)) {
			this.locationInDisk+="/";
		}
		File dbFile = new File(locationInDisk+DB_NAME);
		try {
			// creating database if not exists
			db = SqlJetDb.open(dbFile, true);
			if (!db.getOptions().isAutovacuum()){
				db.getOptions().setAutovacuum(true);
			}
			// creating table if not exists
			db.beginTransaction(SqlJetTransactionMode.WRITE);
			db.getOptions().setUserVersion(1);
			db.createTable(CREATE_BULK_TABLE);
			db.createTable(CREATE_EVENT_TABLE);
			failedBulkTable = db.getTable(FAILED_BULKS_TABLE_NAME);
			overFlowedEventsTable = db.getTable(OVERFLOWED_EVENTS_TABLE_NAME);

			// update kamon gauge (counter)
			KamonConstants.CURRENT_DATA_IN_DB_GAUGE.withTag("type", FAILED_BULKS_TABLE_NAME).update(failedBulksAmount());
			KamonConstants.CURRENT_DATA_IN_DB_GAUGE.withTag("type", OVERFLOWED_EVENTS_TABLE_NAME).update(overFlowedEventsListsAmount());

			silentDbCommit();
			LOG.info("SQLite was created successfully");
		} catch (Exception e) {
			LOG.error("Creation of DB has failed",e);
			silentCloseDb();
		}
	}

	//region public methods

	@Override
	public synchronized List<DbBulkRequest> fetchAndDeleteFailedBulks() {
		return fetchFailedBulks(true);
	}

	@Override
	public synchronized List<Event> fetchAndDeleteOverflowedEvents() {
		List<Event> allEvents = new ArrayList<>();
		ISqlJetCursor resultCursor = null;
		List<Event> events;

		try {
			db.beginTransaction(SqlJetTransactionMode.WRITE);
			resultCursor = overFlowedEventsTable.lookup(overFlowedEventsTable.getPrimaryKeyIndexName());

			for (int i = 0; i < maxFetchedEventsListsInOneTime && !resultCursor.eof() ; i++) {
				events = deserializeEvents(resultCursor.getBlobAsArray(OVERFLOWED_EVENT));
				int eventsSize = events.size();
				LOG.info("Fetched bulk of {} overflowed events from SQLite.", eventsSize);
				allEvents.addAll(events);
				resultCursor.delete(); // also do next
				KamonConstants.CURRENT_DATA_IN_DB_GAUGE.withTag("type", OVERFLOWED_EVENTS_TABLE_NAME).decrement(); // removed events from db
			}
			if (!allEvents.isEmpty()) {
				LOG.info("Overflowed events fetch was successful. Number of fetched events: {}.", allEvents.size());
			}
			else {
				LOG.info("There are no overflowed events to fetch from disk.");
			}
		} catch (Exception e) {
			LOG.error("Fetching of overflowed events has failed.",e);
		} finally {
			closeCursor(resultCursor);
			silentDbCommit();
		}
		return allEvents;
	}

	@Override
	public Future<?> persistBulkRequest(DbBulkRequest dbBulkRequest, int bulkNum) {
		return executorService.submit(() -> {
			try {
				persistBulkRequest(dbBulkRequest, 1000, bulkNum);
			} catch (MaximumInsertTriesException e) {
				LOG.error("Bulk #{} Tasks of failed bulk will not be indexed because couldn't be persisted to disk for the maximum times ({}).", bulkNum, e.getMaximumTriesNumber());
				KamonConstants.TASKS_FETCHED_FROM_DISK_HISTOGRAM.withTag("outcome", "error").record(1);
			}
		});
	}

	@Override
	public synchronized void persistEvents(ArrayList<Event> events) {
		try {
			db.beginTransaction(SqlJetTransactionMode.WRITE);
			overFlowedEventsTable.insert(serializeEvents(events), DateTime.now().toString());
			LOG.info("List of {} overflowed events was inserted successfully to disk.", events.size());
			KamonConstants.CURRENT_DATA_IN_DB_GAUGE.withTag("type", OVERFLOWED_EVENTS_TABLE_NAME).increment();
		} catch (Exception e) {
			LOG.error("Insertion of overflowed events has failed. Events: "+ events.toString() , e);
		} finally {
			silentDbCommit();
		}
	}

	@Override
	public synchronized boolean hasFailedBulks()  {
		boolean returnValue = false;
		ISqlJetCursor resultCursor = null;
		try {
			db.beginTransaction(SqlJetTransactionMode.READ_ONLY);
			resultCursor = failedBulkTable.lookup(failedBulkTable.getPrimaryKeyIndexName());
			returnValue = !resultCursor.eof();
		} catch (Exception e) {
			LOG.error("Failed to check how many bulks are in SQLite.");
		} finally {
			closeCursor(resultCursor);
		}
		return returnValue;
	}

	@Override
	public boolean isCreatedSuccessfully() {
		boolean ret = db != null;
		if (!ret){
			LOG.error("SQLite wasn't initialized successfully.");
		}
		return ret;
	}

	@Override
	public synchronized long failedBulksAmount() {
		return getTableRowCount(failedBulkTable);
	}

	@Override
	public synchronized long overFlowedEventsListsAmount() {
		return getTableRowCount(overFlowedEventsTable);
	}

	private long getTableRowCount(ISqlJetTable table) {
		ISqlJetCursor resultCursor = null;
		try {
			db.beginTransaction(SqlJetTransactionMode.READ_ONLY);
			resultCursor = table.lookup(table.getPrimaryKeyIndexName());
			return resultCursor.getRowCount();
		} catch (SqlJetException e) {
			LOG.error("Table row count has failed.", e);
			return 0;
		} finally {
			closeCursor(resultCursor);
		}
	}

	@Override
	public void close() {
	}

	@Override
	public void reset() {
		try {
			db.dropTable(FAILED_BULKS_TABLE_NAME);
			db.dropTable(OVERFLOWED_EVENTS_TABLE_NAME);
			db.createTable(CREATE_BULK_TABLE);
			db.createTable(CREATE_EVENT_TABLE);
			failedBulkTable = db.getTable(FAILED_BULKS_TABLE_NAME);
			overFlowedEventsTable = db.getTable(OVERFLOWED_EVENTS_TABLE_NAME);
			LOG.info("Recreated table successfully.");
			db.commit();
		} catch (Exception e) {
			LOG.warn("Drop table has failed", e);
		}
	}

	// endregion

	//region package methods

	synchronized void persistBulkRequest(DbBulkRequest dbBulkRequest, long sleepTimeIfFails, int bulkNum) throws MaximumInsertTriesException {
		int timesFetched = dbBulkRequest.getTimesFetched();
		if (timesFetched > 0) {
			LOG.info("Bulk #{} Inserting bulk request with id: {} to disk, that was fetched {} {}.", bulkNum, dbBulkRequest.getId(), timesFetched, timesFetched > 1 ? "times" : "time");
		} else {
			LOG.info("Bulk #{} Inserting bulk request to disk for the first time.", bulkNum);
		}

		for (int tryNum = 1; tryNum <= maxInsertTries; tryNum++) {
			if (tryNum > 1) {
				LOG.info("Bulk #{} Started try # {}/{} to persist a bulk", bulkNum, tryNum, maxInsertTries);
			}
			try {
				db.beginTransaction(SqlJetTransactionMode.WRITE);
				dbBulkRequest.setInsertTime(DateTime.now().toString());
				failedBulkTable.insert(serializeBulkRequest(dbBulkRequest.getRequest()),
						dbBulkRequest.getInsertTime(), timesFetched);
				LOG.info("Bulk #{} Try # {}. Bulk request was inserted successfully to disk.", bulkNum, tryNum);
				KamonConstants.CURRENT_DATA_IN_DB_GAUGE.withTag("type", FAILED_BULKS_TABLE_NAME).increment();
				break; // if arrived here then insertion succeeded, no need to retry again

			} catch (Exception e) {
				LOG.error("Bulk #" + bulkNum + ". Try # " + tryNum + "/" + maxInsertTries + " to persist a bulk has failed.", e);

				try {
					Thread.sleep(sleepTimeIfFails);
				} catch (InterruptedException ex) {
					LOG.error("Failed to sleep after maximum insertion tries to db", e);
				}

				sleepTimeIfFails *= 2;
				if (tryNum >= maxInsertTries) {
					throw new MaximumInsertTriesException(maxInsertTries);
				}
			} finally {
				silentDbCommit();
			}
		}
	}

	List<DbBulkRequest> fetchFailedBulks(boolean deleteAfterFetch) {
		List<DbBulkRequest> dbBulkRequests = new ArrayList<>();
		ISqlJetCursor resultCursor = null;
		DbBulkRequest dbBulkRequest;

		try {
			LOG.info("Fetching failed bulks from SQLite.");
			db.beginTransaction(SqlJetTransactionMode.WRITE);
			resultCursor = failedBulkTable.lookup(failedBulkTable.getPrimaryKeyIndexName());

			for (int i = 0; i < maxFetchedBulksInOneTime && !resultCursor.eof(); i++) {
				dbBulkRequest = createDbBulkRequestFromCursor(resultCursor);
				dbBulkRequests.add(dbBulkRequest);
				if (deleteAfterFetch) {
					resultCursor.delete(); // also do next
					KamonConstants.CURRENT_DATA_IN_DB_GAUGE.withTag("type", FAILED_BULKS_TABLE_NAME).decrement(); // removed request from db
				} else {
					resultCursor.next();
				}
			}
			LOG.info("Failed bulks fetch was successful. Number of fetched bulks: {}.", dbBulkRequests.size());
		} catch (Exception e) {
			LOG.error("Fetching failed bulks has failed.", e);
		} finally {
			closeCursor(resultCursor);
			silentDbCommit();
		}
		return dbBulkRequests;
	}

	private byte[] serializeEvents(ArrayList<Event> events) {
		return SerializationUtils.serialize(events);
	}

	List<Event> deserializeEvents(byte[] blobAsArray) {
		try {
			return SerializationUtils.deserialize(blobAsArray);
		} catch (SerializationException e){
			LOG.error("Error deserializing list of events from DB", e);
			return Collections.emptyList();
		}
	}

	private byte[] serializeBulkRequest(BulkRequest request) throws IOException {
		try (BytesStreamOutput out = new BytesStreamOutput()) {
			request.writeTo(out);
			return out.bytes().toBytesRef().bytes;
		}
	}

	BulkRequest deserializeBulkRequest(byte[] bulkRequestBytes) throws IOException {
		try (StreamInput stream = StreamInput.wrap(bulkRequestBytes)) {
			return new BulkRequest(stream);
		} catch (SerializationException e){
			LOG.error("Error deserializing Bulk request from DB", e);
			return new BulkRequest();
		}
	}

	// endregion


	//region private methods

	private DbBulkRequest createDbBulkRequestFromCursor(ISqlJetCursor resultCursor) throws IOException, SqlJetException {
		BulkRequest request = deserializeBulkRequest(resultCursor.getBlobAsArray(FAILED_TASK));
		DbBulkRequest dbBulkRequest = new DbBulkRequest(request);
		dbBulkRequest.setId(resultCursor.getInteger(ID));
		dbBulkRequest.setInsertTime(resultCursor.getString(INSERT_TIME));
		dbBulkRequest.setTimesFetched((int) resultCursor.getInteger(TIMES_FETCHED)+1); // increment by 1 because we call this method while fetching
		return dbBulkRequest;
	}

	private void closeCursor(ISqlJetCursor cursor) {
		try {
			if (cursor != null) {
				cursor.close();
			}
		} catch (Exception e) {
			LOG.error("Closing cursor has failed",e);
		}
	}

	private void silentDbCommit() {
		try {
			db.commit();
		} catch (Exception e) {
			LOG.error("Commit updates has failed", e);
		}
	}

	private void silentCloseDb() {
		if (db != null) {
			try {
				db.close();
			} catch (Exception e) {
				LOG.error("Closing SQLite has failed", e);
			}
			db = null;
		}
	}

	// endregion

}


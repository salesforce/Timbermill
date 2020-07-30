package com.datorama.oss.timbermill.common.disk;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

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

import com.datorama.oss.timbermill.common.exceptions.MaximumInsertTriesException;
import com.datorama.oss.timbermill.unit.Event;

import kamon.Kamon;
import kamon.metric.Metric;

public class SQLJetDiskHandler implements DiskHandler {
	static final String MAX_FETCHED_BULKS_IN_ONE_TIME = "MAX_FETCHED_BULKS_IN_ONE_TIME";
	static final String MAX_INSERT_TRIES = "MAX_INSERT_TRIES";
	static final String LOCATION_IN_DISK = "LOCATION_IN_DISK";

	private static final String DB_NAME = "timbermillJet.db";
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
	private static final Logger LOG = LoggerFactory.getLogger(SQLJetDiskHandler.class);

	private int maxFetchedBulksInOneTime;
	private int maxInsertTries;
	private String locationInDisk;
	private SqlJetDb db;
	private ISqlJetTable failedBulkTable;
	private ISqlJetTable overFlowedEventsTable;
	private Metric.Gauge currentTasksInDiskGauge = Kamon.gauge("timbermill2.tasks.in.disk.gauge");


	SQLJetDiskHandler(int maxFetchedBulks, int maxInsertTries, String locationInDisk) {
		this.maxFetchedBulksInOneTime = maxFetchedBulks;
		this.maxInsertTries = maxInsertTries;
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
			currentTasksInDiskGauge.withoutTags().update(failedBulksAmount());
			LOG.info("SQLite was created successfully");
			silentDbCommit();
		} catch (Exception e) {
			LOG.error("Creation of DB has failed",e);
			silentCloseDb();
		}
	}

	//region public methods

	@Override
	public List<DbBulkRequest> fetchAndDeleteFailedBulks() {
		return fetchFailedBulks(true);
	}

	@Override public List<Event> fetchAndDeleteOverflowedEvents() {
		return fetchOverflowedEvents(true);
	}

	private List<Event> fetchOverflowedEvents(boolean deleteAfterFetch) {
		List<Event> allEvents = new ArrayList<>();
		ISqlJetCursor resultCursor = null;
		int fetchedCount = 0;
		List<Event> events;

		try {
			db.beginTransaction(SqlJetTransactionMode.WRITE);
			resultCursor = overFlowedEventsTable.lookup(overFlowedEventsTable.getPrimaryKeyIndexName());

			while (fetchedCount < maxFetchedBulksInOneTime && !resultCursor.eof()) {
				LOG.info("Fetching events from SQLite...");
				events = deserializeEvents(resultCursor.getBlobAsArray(OVERFLOWED_EVENT));
				allEvents.addAll(events);
				if (deleteAfterFetch) {
					resultCursor.delete(); // also do next
				}else {
					resultCursor.next();
				}
				fetchedCount += events.size();
			}
			LOG.info("Fetched successfully. Number of fetched events: {}.",fetchedCount);
		} catch (Exception e) {
			LOG.error("Fetching has failed.",e);
		} finally {
			closeCursor(resultCursor);
			silentDbCommit();
		}
		return allEvents;
	}

	@Override
	public void persistBulkRequestToDisk(DbBulkRequest dbBulkRequest) throws MaximumInsertTriesException {
		persistBulkRequestToDisk(dbBulkRequest,1000);
	}

	@Override public void persistEventsToDisk(ArrayList<Event> events) {
		try {
			db.beginTransaction(SqlJetTransactionMode.WRITE);
			overFlowedEventsTable.insert(serializeEvents(events), DateTime.now().toString());
			LOG.info("List of {} overflowed events was inserted successfully to disk.", events.size());
		} catch (Exception e) {
			LOG.error("Insertion of overflowed events has failed. Events: "+ events.toString() , e);
		} finally {
			silentDbCommit();
		}
	}

	private byte[] serializeEvents(ArrayList<Event> events) {
		return SerializationUtils.serialize(events);
	}

	private List<Event> deserializeEvents(byte[] blobAsArray) {
		return SerializationUtils.deserialize(blobAsArray);
	}

	@Override
	public boolean hasFailedBulks()  {
		boolean returnValue = false;
		ISqlJetCursor resultCursor = null;
		try {
			db.beginTransaction(SqlJetTransactionMode.READ_ONLY);
			resultCursor = failedBulkTable.lookup(failedBulkTable.getPrimaryKeyIndexName());
			returnValue = !resultCursor.eof();
		} catch (Exception e) {
			LOG.error("Checking how many bulks are in SQLite has failed.");
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

	@Override public void close() {
		try {
			db.dropTable(FAILED_BULKS_TABLE_NAME);
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

	@Override public long failedBulksAmount() {
		try {
			ISqlJetCursor resultCursor = failedBulkTable.lookup(failedBulkTable.getPrimaryKeyIndexName());
			return resultCursor.getRowCount();
		} catch (SqlJetException e) {
			LOG.error("Table row count has failed.",e);
			return 0;
		}
	}

	// endregion

	//region package methods

	void persistBulkRequestToDisk(DbBulkRequest dbBulkRequest, long sleepTimeIfFails) throws MaximumInsertTriesException {
		int retryNum = 0;

		while (retryNum++ < maxInsertTries) {
			try {

				if (dbBulkRequest.getTimesFetched() > 0) {
					int timesFetched = dbBulkRequest.getTimesFetched();
					LOG.info("Inserting bulk request with id: {} to disk, that was fetched {} {}.", dbBulkRequest.getId(), timesFetched, timesFetched > 1 ? "times" : "time");
				} else {
					LOG.info("Inserting bulk request to disk for the first time.");
				}

				db.beginTransaction(SqlJetTransactionMode.WRITE);
				dbBulkRequest.setInsertTime(DateTime.now().toString());
				failedBulkTable.insert(serializeBulkRequest(dbBulkRequest.getRequest()),
						dbBulkRequest.getInsertTime(), dbBulkRequest.getTimesFetched());
				LOG.info("Bulk request was inserted successfully to disk.");
				currentTasksInDiskGauge.withoutTags().increment();
				break; // if arrived here then insertion succeeded, no need to retry again

			} catch (Exception e) {
				LOG.error("Insertion of bulk has failed for the {}th time. Error message: {}",retryNum, e);
				silentThreadSleep(sleepTimeIfFails);
				sleepTimeIfFails*=2;
				if (retryNum == maxInsertTries){
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
		int fetchedCount = 0;
		DbBulkRequest dbBulkRequest;

		try {
			LOG.info("Fetching from SQLite...");
			db.beginTransaction(SqlJetTransactionMode.WRITE);
			resultCursor = failedBulkTable.lookup(failedBulkTable.getPrimaryKeyIndexName());

			while (fetchedCount < maxFetchedBulksInOneTime && !resultCursor.eof()) {
				dbBulkRequest = createDbBulkRequestFromCursor(resultCursor);
				dbBulkRequests.add(dbBulkRequest);
				if (deleteAfterFetch) {
					resultCursor.delete(); // also do next
				}else {
					resultCursor.next();
				}
				fetchedCount++;
				currentTasksInDiskGauge.withoutTags().decrement(); // removed request from db
			}
			LOG.info("Fetched successfully. Number of fetched bulks: {}.",fetchedCount);
		} catch (Exception e) {
			LOG.error("Fetching has failed.",e);
		} finally {
			closeCursor(resultCursor);
			silentDbCommit();
		}
		return dbBulkRequests;
	}

	// endregion


	//region private methods

	private DbBulkRequest createDbBulkRequestFromCursor(ISqlJetCursor resultCursor) throws IOException, SqlJetException {
		BulkRequest request = deserializeBulkRequest(resultCursor.getBlobAsArray(FAILED_TASK));
		DbBulkRequest dbBulkRequest = new DbBulkRequest(request);
		dbBulkRequest.setId((int) resultCursor.getInteger(ID));
		dbBulkRequest.setInsertTime(resultCursor.getString(INSERT_TIME));
		dbBulkRequest.setTimesFetched((int) resultCursor.getInteger(TIMES_FETCHED)+1); // increment by 1 because we call this method while fetching
		return dbBulkRequest;
	}

	private byte[] serializeBulkRequest(BulkRequest request) throws IOException {
		try (BytesStreamOutput out = new BytesStreamOutput()) {
			request.writeTo(out);
			return out.bytes().toBytesRef().bytes;
		}
	}

	private BulkRequest deserializeBulkRequest(byte[] bulkRequestBytes) throws IOException {
		StreamInput stream = null;
		try {
			BulkRequest request = new BulkRequest();
			stream = StreamInput.wrap(bulkRequestBytes);
			request.readFrom(stream);
			return request;
		}finally {
			if (stream!=null){
				stream.close();
			}
		}
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

	private void silentDbCommit(){
		try {
			db.commit();
		} catch (Exception e) {
			LOG.error("Commit updates has failed",e);
		}
	}

	private void silentCloseDb()  {
		if (db!=null){
			try {
				db.close();
			} catch (Exception e) {
				LOG.error("Closing SQLite has failed",e);
			}
			db = null;
		}
	}

	private void silentThreadSleep(long sleepTime) {
		try {
			Thread.sleep(sleepTime);
		} catch (Exception e) {
			LOG.warn("Making thread sleep has failed",e);
		}
	}

	// endregion

}


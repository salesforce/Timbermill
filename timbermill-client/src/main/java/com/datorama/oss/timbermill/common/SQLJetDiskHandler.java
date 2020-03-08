package com.datorama.oss.timbermill.common;

import java.io.*;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.SqlJetTransactionMode;
import org.tmatesoft.sqljet.core.table.ISqlJetCursor;
import org.tmatesoft.sqljet.core.table.ISqlJetTable;
import org.tmatesoft.sqljet.core.table.SqlJetDb;


public class SQLJetDiskHandler implements DiskHandler {
	private static final String DB_NAME = "timbermillJet.db";
	private static final String FAILED_BULKS_TABLE_NAME = "failed_bulks";
	private static final String ID = "id";
	private static final String FAILED_TASK = "failedTask";
	private static final String CREATE_TIME = "createTime";
	private static final String INSERT_TIME = "insertTime";
	private static final String TIMES_FETCHED = "timesFetched";
	private static final String IN_DISK = "inDisk";
	private static final String INSERT_TIME_INDEX = "insertTimeIndex";

	public static final int fetchLimit = 10;
	public static final long waitingTime = 1*60000; // 1 minute


	private static SqlJetDb db;
	private static ISqlJetTable table;

	private static final String CREATE_TABLE =
			"CREATE TABLE IF NOT EXISTS " + FAILED_BULKS_TABLE_NAME + " (" + ID + " TEXT PRIMARY KEY, " + FAILED_TASK + " BLOB NOT NULL, " + CREATE_TIME + " DATE, " + INSERT_TIME + " LONG, "
					+ TIMES_FETCHED + " INTEGER, " + IN_DISK + " BOOLEAN)";
	private static final String createInsertTimeIndexQuery = "CREATE INDEX " + INSERT_TIME_INDEX + " ON " + FAILED_BULKS_TABLE_NAME + "(" +  INSERT_TIME + ")";

	private static final Logger LOG = LoggerFactory.getLogger(SQLJetDiskHandler.class);

	// initializing db
	static {
		File dbFile = new File(DB_NAME);
		dbFile.delete();
		try {
			// creating database
			db = SqlJetDb.open(dbFile, true);
			db.getOptions().setAutovacuum(true);
			// creating table
			db.beginTransaction(SqlJetTransactionMode.WRITE);
			db.getOptions().setUserVersion(1);
			db.createTable(CREATE_TABLE);
			table = db.getTable(FAILED_BULKS_TABLE_NAME);
			db.createIndex(createInsertTimeIndexQuery);

			db.commit();
			LOG.info("Created DB successfully " + CREATE_TABLE);
		} catch (SqlJetException e) {
			e.printStackTrace();
		}
	}

	//region public methods

	public SQLJetDiskHandler() {
	}

	public List<DbBulkRequest> fetchFailedBulks(boolean deleteAfterFetch) {
		List<DbBulkRequest> dbBulkRequests = new ArrayList<>();
		ISqlJetCursor resultCursor = null;
		try {
			LOG.info("**running query");
			db.beginTransaction(SqlJetTransactionMode.READ_ONLY);
			resultCursor = table.scope(INSERT_TIME_INDEX, new Object[] {0}, new Object[] {System.currentTimeMillis()-waitingTime}); // bulk is in db at least waitingTime

			if (!resultCursor.eof()) {

				int fetchedCount = 0;
				DbBulkRequest dbBulkRequest;
				BulkRequest request;

				do {
					request = deserializeBulkRequest(resultCursor.getBlobAsArray(FAILED_TASK));
					dbBulkRequest = new DbBulkRequest(request);
					dbBulkRequest.setId(resultCursor.getString(ID));
					dbBulkRequest.setCreateTime((Date) resultCursor.getValue(CREATE_TIME));
					dbBulkRequest.setInsertTime((Long)resultCursor.getValue(INSERT_TIME));
					dbBulkRequest.setTimesFetched((int) resultCursor.getInteger(TIMES_FETCHED)+1);
					dbBulkRequest.setInDisk(true);
					dbBulkRequests.add(dbBulkRequest);
					if (deleteAfterFetch){
						resultCursor.delete();
					}
				} while (resultCursor.next() && ++fetchedCount < fetchLimit);
			}
		} catch (SqlJetException | IOException e) {
			e.printStackTrace();
		} finally {
			closeCursor(resultCursor);
		}
		return dbBulkRequests;
	}

		@Override public List<DbBulkRequest> fetchFailedBulks() {
			return fetchFailedBulks(true);
		}

		@Override public void persistToDisk(DbBulkRequest dbBulkRequest) {
		try {
			LOG.info("running insert of bulk request with id: {}",dbBulkRequest.getId());
			db.beginTransaction(SqlJetTransactionMode.WRITE);
			ISqlJetTable table = db.getTable(FAILED_BULKS_TABLE_NAME);
			long currentTime = System.currentTimeMillis();
			table.insert(dbBulkRequest.getId(), serializeBulkRequest(dbBulkRequest.getRequest()), dbBulkRequest.getCreateTime(),
					currentTime, dbBulkRequest.getTimesFetched(), true);
			dbBulkRequest.setInDisk(true);
			dbBulkRequest.setInsertTime(currentTime);
			db.commit();
		} catch (SqlJetException | IOException e) {
			e.printStackTrace();
		}
	}

	@Override public void deleteBulk(DbBulkRequest dbBulkRequest) {
		String id = dbBulkRequest.getId();
		LOG.info("running delete of bulk request with id: {}",id);
		try {
			db.beginTransaction(SqlJetTransactionMode.WRITE);
			ISqlJetCursor deleteCursor = table.lookup(table.getPrimaryKeyIndexName(), id);
			if (!deleteCursor.eof()) {
				deleteCursor.delete();
			}
			deleteCursor.close();
			db.commit();

		} catch (SqlJetException e) {
			e.printStackTrace();
		}
	}

	@Override public void updateBulk(String id, DbBulkRequest dbBulkRequest) {
		try {
			LOG.info("running update");
			db.beginTransaction(SqlJetTransactionMode.WRITE);
			ISqlJetCursor updateCursor = table.lookup(table.getPrimaryKeyIndexName(), id);
			if (!updateCursor.eof()) {
				updateCursor.update(id, serializeBulkRequest(dbBulkRequest.getRequest()), dbBulkRequest.getCreateTime(),
						dbBulkRequest.getInsertTime(), dbBulkRequest.getTimesFetched(), true);
			}
			updateCursor.close();
			db.commit();
		} catch (SqlJetException | IOException ex) {
			ex.printStackTrace();
		}
	}

	public void dropTable(){
		try {
			db.dropTable(FAILED_BULKS_TABLE_NAME);
			db.commit();
		} catch (SqlJetException e) {
			e.printStackTrace();
		}
	}

	public void emptyDb() {
		LOG.info("empties db");
		try {
			db.beginTransaction(SqlJetTransactionMode.WRITE);
			ISqlJetCursor deleteCursor = table.lookup(table.getPrimaryKeyIndexName());
			while (!deleteCursor.eof()) {
				deleteCursor.delete();
			}
			deleteCursor.close();
			db.commit();

		} catch (SqlJetException e) {
			e.printStackTrace();
		}
	}

	public int failedBulksAmount() {
		return fetchFailedBulks(false).size();
	}
	// endregion



	//region private methods

	private byte[] serializeBulkRequest(BulkRequest request) throws IOException {
		BytesStreamOutput out = new BytesStreamOutput();
		try {
			request.writeTo(out);
			return out.bytes().toBytesRef().bytes;
		} finally {
			out.close();
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
			stream.close();
		}
	}

	private void closeCursor(ISqlJetCursor cursor) {
		try {
			if (cursor != null) {
				cursor.close();
			}
		} catch (SqlJetException e) {
			e.printStackTrace();
		}
	}

	// endregion

}


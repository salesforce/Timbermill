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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
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
	private static final String INSERT_TIME_INDEX = "insertTimeIndex";
	 @Value("${LOCATION_IN_DISK:}")
	 private static String locationInDisk;

	public final int fetchLimit = 10;
	public final long waitingTime;

	private static SqlJetDb db;
	private static ISqlJetTable table;

	private static final String CREATE_TABLE =
			"CREATE TABLE IF NOT EXISTS " + FAILED_BULKS_TABLE_NAME + " (" + ID + " TEXT PRIMARY KEY, " + FAILED_TASK + " BLOB NOT NULL, " + CREATE_TIME + " TEXT, " + INSERT_TIME + " LONG, "
					+ TIMES_FETCHED + " INTEGER)";
	private static final String createInsertTimeIndexQuery = "CREATE INDEX IF NOT EXISTS " + INSERT_TIME_INDEX + " ON " + FAILED_BULKS_TABLE_NAME + "(" +  INSERT_TIME + ")";

	private static final Logger LOG = LoggerFactory.getLogger(SQLJetDiskHandler.class);
	//public static final boolean exponentialBackoff = false;

	// initializing database
	static {
		File dbFile = new File(DB_NAME);
		try {
			// creating database if not exists
			db = SqlJetDb.open(dbFile, true);
			if (!db.getOptions().isAutovacuum()){
				db.getOptions().setAutovacuum(true);
			}
			// creating table if not exists
			db.beginTransaction(SqlJetTransactionMode.WRITE);
			db.getOptions().setUserVersion(1);
			db.createTable(CREATE_TABLE);
			table = db.getTable(FAILED_BULKS_TABLE_NAME);
			// creating index if not exists
			db.createIndex(createInsertTimeIndexQuery);
			LOG.info("Created DB successfully: " + CREATE_TABLE);
		} catch (SqlJetException e) {
			LOG.error("Creating DB has failed",e);
		} finally {
			silentDbCommit();
		}
	}

	//region public methods

	public SQLJetDiskHandler() {
		this.waitingTime = 1*60000; // 1 minute
	}

	public SQLJetDiskHandler(long waitingTime) {
		this.waitingTime = waitingTime;
	}

	@Override public List<DbBulkRequest> fetchFailedBulks() {
		return fetchFailedBulks(true);
	}

	@Override public void persistToDisk(DbBulkRequest dbBulkRequest) {
		try {
			LOG.info("running insert of bulk request with id: {}, that was fetched {} times.", dbBulkRequest.getId(), dbBulkRequest.getTimesFetched());
			db.beginTransaction(SqlJetTransactionMode.WRITE);
			ISqlJetTable table = db.getTable(FAILED_BULKS_TABLE_NAME);
			long currentTime = System.currentTimeMillis();
			table.insert(dbBulkRequest.getId(), serializeBulkRequest(dbBulkRequest.getRequest()), dbBulkRequest.getCreateTime(),
					currentTime, dbBulkRequest.getTimesFetched());
			dbBulkRequest.setInsertTime(currentTime);
		} catch (SqlJetException | IOException e) {
			LOG.error("Insertion of bulk {} has failed.", dbBulkRequest.getId(),e);
		} finally {
			silentDbCommit();
		}
	}

	@Override public void deleteBulk(DbBulkRequest dbBulkRequest) {
		String id = dbBulkRequest.getId();
		ISqlJetCursor deleteCursor = null;
		LOG.info("running delete of bulk request with id: {}", id);
		try {
			db.beginTransaction(SqlJetTransactionMode.WRITE);
			deleteCursor = table.lookup(table.getPrimaryKeyIndexName(), id);
			if (!deleteCursor.eof()) {
				deleteCursor.delete();
			}

		} catch (SqlJetException e) {
			LOG.error("deletion of bulk {} has failed.", dbBulkRequest.getId(),e);
		} finally {
			silentDbCommit();
			closeCursor(deleteCursor);
		}
	}

	@Override public void updateBulk(String id, DbBulkRequest dbBulkRequest) {
		ISqlJetCursor updateCursor = null;
		try {
			LOG.info("running update");
			db.beginTransaction(SqlJetTransactionMode.WRITE);
			updateCursor = table.lookup(table.getPrimaryKeyIndexName(), id);
			if (!updateCursor.eof()) {
				updateCursor.update(id, serializeBulkRequest(dbBulkRequest.getRequest()), dbBulkRequest.getCreateTime(),
						dbBulkRequest.getInsertTime(), dbBulkRequest.getTimesFetched());
			}
			updateCursor.close();
		} catch (SqlJetException | IOException e) {
			LOG.error("updating of bulk {} has failed.", dbBulkRequest.getId(),e);
		} finally {
			silentDbCommit();
			closeCursor(updateCursor);
		}
	}

	@Override
	public boolean hasFailedBulks()  {
		boolean returnValue = false;
		ISqlJetCursor resultCursor = null;
		try {
			db.beginTransaction(SqlJetTransactionMode.READ_ONLY);
			resultCursor = table.scope(INSERT_TIME_INDEX, new Object[] {0}, new Object[] {System.currentTimeMillis()-waitingTime}); // bulk is in db at least waitingTime
			returnValue = !resultCursor.eof();
		} catch (SqlJetException e) {
			e.printStackTrace();
		} finally {
			closeCursor(resultCursor);
			return returnValue;
		}
	}



	public void emptyDb() {
		LOG.info("empties db");
		ISqlJetCursor deleteCursor = null;
		try {
			db.beginTransaction(SqlJetTransactionMode.WRITE);
			deleteCursor = table.lookup(table.getPrimaryKeyIndexName());
			while (!deleteCursor.eof()) {
				deleteCursor.delete();
			}
			deleteCursor.close();

		} catch (SqlJetException e) {
			LOG.error("emptying the db {} has failed",DB_NAME,e);
		} finally {
			silentDbCommit();
			closeCursor(deleteCursor);
		}
	}

	public int failedBulksAmount() {
		return fetchFailedBulks(false).size();
	}

	public void dropTable(){
		try {
			db.dropTable(FAILED_BULKS_TABLE_NAME);
			db.commit();
		} catch (SqlJetException e) {
			LOG.error("dropping the table {} has failed",FAILED_BULKS_TABLE_NAME,e);
		}
	}
	// endregion



	//region private methods

	List<DbBulkRequest> fetchFailedBulks(boolean deleteAfterFetch) {
		List<DbBulkRequest> dbBulkRequests = new ArrayList<>();
		ISqlJetCursor resultCursor = null;
		int fetchedCount = 0;
		DbBulkRequest dbBulkRequest;

		try {
			LOG.info("running fetch.");
			db.beginTransaction(SqlJetTransactionMode.READ_ONLY);
			resultCursor = table.scope(INSERT_TIME_INDEX, new Object[] {0}, new Object[] {System.currentTimeMillis()-waitingTime}); // bulk is in db at least waitingTime

			if (!resultCursor.eof()) {

				do {
					dbBulkRequest = createDbBulkRequestFromCursor(resultCursor);
					dbBulkRequests.add(dbBulkRequest);
					if (deleteAfterFetch){
						resultCursor.delete();
					}
				} while (++fetchedCount < fetchLimit && resultCursor.next());
				LOG.info("fetched {} bulk requests.",fetchedCount);
			}
		} catch (SqlJetException | IOException e) {
			LOG.error("fetching has failed.",e);
		} finally {
			closeCursor(resultCursor);
		}
		return dbBulkRequests;
	}

	private DbBulkRequest createDbBulkRequestFromCursor(ISqlJetCursor resultCursor) throws IOException, SqlJetException {
		BulkRequest request = deserializeBulkRequest(resultCursor.getBlobAsArray(FAILED_TASK));
		DbBulkRequest dbBulkRequest = new DbBulkRequest(request);
		dbBulkRequest.setId(resultCursor.getString(ID));
		dbBulkRequest.setCreateTime(resultCursor.getString(CREATE_TIME));
		dbBulkRequest.setInsertTime((Long)resultCursor.getValue(INSERT_TIME));
		dbBulkRequest.setTimesFetched((int) resultCursor.getInteger(TIMES_FETCHED)+1); // increment by 1 because we call this method while fetching
		return dbBulkRequest;
	}

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
			LOG.error("closing cursor {} has failed",e);
		}
	}

	private static void silentDbCommit(){
		try {
			db.commit();
		} catch (SqlJetException e) {
			LOG.error("the commit has failed",e);
		}
	}
	// endregion

}


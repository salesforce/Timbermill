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
import org.springframework.stereotype.Service;


@Service
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
	//public static final boolean exponentialBackoff = false;



	private static SqlJetDb db;
	private static ISqlJetTable table;

	private static final String CREATE_TABLE =
			"CREATE TABLE IF NOT EXISTS " + FAILED_BULKS_TABLE_NAME + " (" + ID + " TEXT PRIMARY KEY, " + FAILED_TASK + " BLOB NOT NULL, " + CREATE_TIME + " DATE, " + INSERT_TIME + " LONG, "
					+ TIMES_FETCHED + " INTEGER, " + IN_DISK + " BOOLEAN)";
	private static final String createInsertTimeIndexQuery = "CREATE INDEX IF NOT EXISTS " + INSERT_TIME_INDEX + " ON " + FAILED_BULKS_TABLE_NAME + "(" +  INSERT_TIME + ")";

	private static final Logger LOG = LoggerFactory.getLogger(SQLJetDiskHandler.class);

	// initializing db
	static {
		File dbFile = new File(DB_NAME);
		//dbFile.delete();
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

			LOG.info("Created DB successfully " + CREATE_TABLE);
		} catch (SqlJetException e) {
			e.printStackTrace();
		} finally {
			silentDbCommit();
		}
	}

	//region public methods

	public SQLJetDiskHandler() {
	}

	public List<DbBulkRequest> fetchFailedBulks(boolean deleteAfterFetch) {
		List<DbBulkRequest> dbBulkRequests = new ArrayList<>();
		ISqlJetCursor resultCursor = null;
		int fetchedCount = 0;
		DbBulkRequest dbBulkRequest;

		try {
			LOG.info("**running query");
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
			e.printStackTrace();
		} finally {
			closeCursor(resultCursor);
		}
		return dbBulkRequests;
	}

	private DbBulkRequest createDbBulkRequestFromCursor(ISqlJetCursor resultCursor) throws IOException, SqlJetException {
		BulkRequest request = deserializeBulkRequest(resultCursor.getBlobAsArray(FAILED_TASK));
		DbBulkRequest dbBulkRequest = new DbBulkRequest(request);
		dbBulkRequest.setId(resultCursor.getString(ID));
		dbBulkRequest.setCreateTime((Date) resultCursor.getValue(CREATE_TIME));
		dbBulkRequest.setInsertTime((Long)resultCursor.getValue(INSERT_TIME));
		dbBulkRequest.setTimesFetched((int) resultCursor.getInteger(TIMES_FETCHED)+1); // increment by 1 because we call this method while fetching
		dbBulkRequest.setInDisk(true);
		return dbBulkRequest;
	}

	@Override public List<DbBulkRequest> fetchFailedBulks() {
			return fetchFailedBulks(true);
		}

		@Override public void persistToDisk(DbBulkRequest dbBulkRequest) {
		try {
			LOG.info("running insert of bulk request with id: {}, that was fetched {} times.",dbBulkRequest.getId(),dbBulkRequest.getTimesFetched());
			db.beginTransaction(SqlJetTransactionMode.WRITE);
			ISqlJetTable table = db.getTable(FAILED_BULKS_TABLE_NAME);
			long currentTime = System.currentTimeMillis();
			table.insert(dbBulkRequest.getId(), serializeBulkRequest(dbBulkRequest.getRequest()), dbBulkRequest.getCreateTime(),
					currentTime, dbBulkRequest.getTimesFetched(), true);
			dbBulkRequest.setInDisk(true);
			dbBulkRequest.setInsertTime(currentTime);
		} catch (SqlJetException | IOException e) {
			e.printStackTrace();
		} finally {
			silentDbCommit();
		}
	}

	@Override public void deleteBulk(DbBulkRequest dbBulkRequest) {
		String id = dbBulkRequest.getId();
		ISqlJetCursor deleteCursor = null;
		LOG.info("running delete of bulk request with id: {}",id);
		try {
			db.beginTransaction(SqlJetTransactionMode.WRITE);
			 deleteCursor = table.lookup(table.getPrimaryKeyIndexName(), id);
			if (!deleteCursor.eof()) {
				deleteCursor.delete();
			}

		} catch (SqlJetException e) {
			e.printStackTrace();
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
						dbBulkRequest.getInsertTime(), dbBulkRequest.getTimesFetched(), true);
			}
			updateCursor.close();
		} catch (SqlJetException | IOException ex) {
			ex.printStackTrace();
		} finally {
			silentDbCommit();
			closeCursor(updateCursor);
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
		ISqlJetCursor deleteCursor = null;
		try {
			db.beginTransaction(SqlJetTransactionMode.WRITE);
			deleteCursor = table.lookup(table.getPrimaryKeyIndexName());
			while (!deleteCursor.eof()) {
				deleteCursor.delete();
			}
			deleteCursor.close();

		} catch (SqlJetException e) {
			e.printStackTrace();
		} finally {
			silentDbCommit();
			closeCursor(deleteCursor);
		}
	}

	public int failedBulksAmount() {
		return fetchFailedBulks(false).size();
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

	private static void silentDbCommit(){
		try {
			db.commit();
		} catch (SqlJetException e) {
			e.printStackTrace();
		}
	}
	// endregion

}


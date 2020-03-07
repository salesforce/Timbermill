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
	private static final String UPDATE_TIME = "updateTime";
	private static final String TIMES_FETCHED = "timesFetched";
	private static final String IN_DISK = "inDisk";

	private static SqlJetDb db;
	private static ISqlJetTable table;

	private static final String CREATE_TABLE =
			"CREATE TABLE IF NOT EXISTS " + FAILED_BULKS_TABLE_NAME + " (" + ID + " TEXT PRIMARY KEY, " + FAILED_TASK + " BLOB NOT NULL, " + CREATE_TIME + " TEXT, " + UPDATE_TIME + " TEXT, "
					+ TIMES_FETCHED + " INTEGER, " + IN_DISK + " BOOLEAN)";

	private static final Logger LOG = LoggerFactory.getLogger(SQLJetDiskHandler.class);

	// initializing db
	static {
		File dbFile = new File(DB_NAME);
		dbFile.delete();
		try {
			// create database
			db = SqlJetDb.open(dbFile, true);
			db.getOptions().setAutovacuum(true);
			// create table
			db.beginTransaction(SqlJetTransactionMode.WRITE);
			db.getOptions().setUserVersion(1);
			db.createTable(CREATE_TABLE);
			table = db.getTable(FAILED_BULKS_TABLE_NAME);
			db.commit();
			LOG.info("Created DB successfully " + CREATE_TABLE);
		} catch (SqlJetException e) {
			e.printStackTrace();
		}
	}

	public SQLJetDiskHandler() {
	}

	@Override public List<DbBulkRequest> fetchFailedBulks() {
		List<DbBulkRequest> dbBulkRequests = new ArrayList<>();
		BulkRequest request;
		DbBulkRequest dbBulkRequest;
		ISqlJetCursor resultCursor = null;

		try {
			LOG.info("**running query");
			db.beginTransaction(SqlJetTransactionMode.READ_ONLY);
			resultCursor = table.order(table.getPrimaryKeyIndexName());

			if (!resultCursor.eof()) {
				do {

					byte[] bulkRequestBytes = resultCursor.getBlobAsArray(FAILED_TASK);
					request = deserializeBulkRequest(bulkRequestBytes);
					dbBulkRequest = new DbBulkRequest(request);
					dbBulkRequest.setId(resultCursor.getString(ID));
					dbBulkRequest.setCreateTime((Date) resultCursor.getValue(CREATE_TIME));
					dbBulkRequest.setUpdateTime((Date) resultCursor.getValue(UPDATE_TIME));
					dbBulkRequest.setTimesFetched((int) resultCursor.getInteger(TIMES_FETCHED));
					dbBulkRequest.setInDisk(true);

					dbBulkRequests.add(dbBulkRequest);
				} while (resultCursor.next());
			}
		} catch (SqlJetException | IOException e) {
			e.printStackTrace();
		} finally {
			closeCursor(resultCursor);
		}
		return dbBulkRequests;
	}

	@Override public void persistToDisk(DbBulkRequest dbBulkRequest) {
		try {
			LOG.info("running insert of bulk request with id: {}",dbBulkRequest.getId());
			db.beginTransaction(SqlJetTransactionMode.WRITE);
			ISqlJetTable table = db.getTable(FAILED_BULKS_TABLE_NAME);
			table.insert(dbBulkRequest.getId(), serializeBulkRequest(dbBulkRequest.getRequest()), dbBulkRequest.getCreateTime(),
					dbBulkRequest.getUpdateTime(), dbBulkRequest.getTimesFetched(), true);
			dbBulkRequest.setInDisk(true);
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
			ISqlJetCursor deleteCursor = table.lookup(ID, id);
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
			ISqlJetCursor updateCursor = table.lookup(ID, id);
			if (!updateCursor.eof()) {
				updateCursor.update(id, dbBulkRequest.getRequest(), dbBulkRequest.getCreateTime(),
						dbBulkRequest.getUpdateTime(), dbBulkRequest.getUpdateTime(), true);
			}
			updateCursor.close();
			db.commit();
		} catch (SqlJetException ex) {
			ex.printStackTrace();
		}
	}

	// help methods

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

	public void dropTable(){
		try {
			db.dropTable(FAILED_BULKS_TABLE_NAME);
			db.commit();
		} catch (SqlJetException e) {
			e.printStackTrace();
		}
	}
}


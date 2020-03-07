package com.datorama.oss.timbermill.common;


import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqLiteDiskHandler implements DiskHandler {
	private static final String DB_NAME = "/Users/ozafar/IdeaProjects/Timbermill/timbermill-server/timbermill.db";
	private static final String FAILED_BULKS_TABLE_NAME = "failed_bulks";
	private static final String ID = "id";
	private static final String FAILED_TASK = "failedTask";
	private static final String CREATE_TIME = "createTime";
	private static final String UPDATE_TIME = "updateTime";
	private static final String TIMES_FETCHED = "timesFetched";
	private static final String IN_PROGRESS = "inProgress";
	//TODO check if removing UNIQUE before AUTOINCREMENT is OK
	private static final String CREATE_TABLE =
			"CREATE TABLE IF NOT EXISTS " + FAILED_BULKS_TABLE_NAME + " (" + ID	+ " TEXT, " + FAILED_TASK + " BLOB NOT NULL, " + CREATE_TIME + " TEXT, " + UPDATE_TIME + " TEXT, "	+ TIMES_FETCHED + " INTEGER, " + IN_PROGRESS + " BOOLEAN)";

	private static final Logger LOG = LoggerFactory.getLogger(SqLiteDiskHandler.class);
	private static final String INSERT = "INSERT INTO " + FAILED_BULKS_TABLE_NAME + "(" + ID + ", " + FAILED_TASK + ", " + CREATE_TIME + ", " + UPDATE_TIME	+ ", " + TIMES_FETCHED + ", " + IN_PROGRESS
			+ ") VALUES(?, ?, ?, ?, ?, ?)";
	private static final String QUERY = "SELECT * FROM failed_bulks";
	private static final String DELETE = "DELETE FROM failed_bulks WHERE id = ?";
	public static final String URL = "jdbc:sqlite:" + DB_NAME;


	public SqLiteDiskHandler() throws SQLException {
		try (Connection conn = DriverManager.getConnection(URL) ; Statement stmt = conn.createStatement()){
			LOG.info("Creating DB " + CREATE_TABLE );
			stmt.execute(CREATE_TABLE);
		} catch (Exception e){
			e.printStackTrace();
		}
	}

//	@Override void List<TimbermillBulkRequest> deleteBulk() {
//
//		try (Connection conn = DriverManager.getConnection(URL);
//				PreparedStatement pstmt = conn.prepareStatement(INSERT)) {
//
//			LOG.info("**running delete " + DELETE);
//
//			// set the corresponding param
//			pstmt.setInt(1, id);
//			// execute the delete statement
//			pstmt.executeUpdate();
//		}
//	}

	@Override public List<DbBulkRequest> fetchFailedBulks() {
		List<DbBulkRequest> dbBulkRequests = new ArrayList<>();

		try (Connection conn = DriverManager.getConnection(URL);
				Statement stmt  = conn.createStatement();
				ResultSet rs    = stmt.executeQuery(QUERY)){

			LOG.info("**running query " + QUERY);

			// loop through the result set
			BulkRequest request;
			DbBulkRequest dbBulkRequest;

			while (rs.next()) { // TODO LIMIT IT
				// fetch the serialized object to a byte array
				byte[] st = (byte[])rs.getObject(2);
				request = new BulkRequest();
				request.readFrom(StreamInput.wrap(st));
				dbBulkRequest = new DbBulkRequest(request);
				dbBulkRequest.setId(rs.getString(1));
				dbBulkRequest.setCreateTime(rs.getDate(3));
				dbBulkRequest.setUpdateTime(rs.getDate(4));
				dbBulkRequest.setTimesFetched(rs.getInt(5));
				dbBulkRequest.setInDisk(rs.getBoolean(6));

				dbBulkRequests.add(dbBulkRequest);
			}

		} catch (SQLException e) {
			System.out.println(e.getMessage());

		} catch (IOException e) {
			e.printStackTrace();
		}
		return dbBulkRequests;
	}

	@Override public void persistToDisk(DbBulkRequest dbBulkRequest) {
		BulkRequest request = dbBulkRequest.getRequest();
		try (Connection conn = DriverManager.getConnection(URL) ; PreparedStatement pstmt = conn.prepareStatement(INSERT)) {

			LOG.info("running insert " + INSERT);

			BytesStreamOutput out = new BytesStreamOutput();
			request.writeTo(out);
			pstmt.setString(1, dbBulkRequest.getId());
			pstmt.setBytes(2, out.bytes().toBytesRef().bytes);
			pstmt.setDate(3, dbBulkRequest.getCreateTime());
			pstmt.setDate(4, dbBulkRequest.getUpdateTime());
			pstmt.setInt(5, dbBulkRequest.getTimesFetched());
			pstmt.setBoolean(6, dbBulkRequest.isInDisk());
			pstmt.executeUpdate();
		} catch (Exception e) {
			LOG.error("Failed persisting bulk request to disk. Request: " + dbBulkRequest.getRequest().requests().toString());
			throw new RuntimeException(e);
		}
	}

	@Override public void deleteBulk(DbBulkRequest dbBulkRequest) {

	}

	@Override public void updateBulk(String id, DbBulkRequest dbBulkRequest) {

	}

}


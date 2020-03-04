package com.datorama.oss.timbermill.common;


import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import javax.sql.rowset.serial.SerialBlob;

import org.apache.commons.lang3.SerializationUtils;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
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
			"CREATE TABLE IF NOT EXISTS " + FAILED_BULKS_TABLE_NAME + " (" + ID	+ " INTEGER PRIMARY KEY AUTOINCREMENT, " + FAILED_TASK + " BLOB NOT NULL, " + CREATE_TIME + " TEXT, " + UPDATE_TIME + " TEXT, "	+ TIMES_FETCHED + " INTEGER, " + IN_PROGRESS + " BOOLEAN)";

	private static final Logger LOG = LoggerFactory.getLogger(SqLiteDiskHandler.class);
	private static final String INSERT = "INSERT INTO " + FAILED_BULKS_TABLE_NAME + "(" + ID + ", " + FAILED_TASK + ", " + CREATE_TIME + ", " + UPDATE_TIME	+ ", " + TIMES_FETCHED + ", " + IN_PROGRESS
			+ ") VALUES(?, ?, ?, ?, ?, ?)";
	private static final String QUERY = "SELECT * FROM failed_bulks";
	public static final String URL = "jdbc:sqlite:" + DB_NAME;


	public SqLiteDiskHandler() throws SQLException {
		try (Connection conn = DriverManager.getConnection(URL) ; Statement stmt = conn.createStatement()){
			LOG.info("Creating DB " + CREATE_TABLE );
			stmt.execute(CREATE_TABLE);
		} catch (Exception e){
			e.printStackTrace();
		}
	}

	@Override public List<TimbermillBulkRequest> fetchFailedBulks() {
		List<TimbermillBulkRequest> timbermillBulkRequests = new ArrayList<>();

		try (Connection conn = DriverManager.getConnection(URL);
				Statement stmt  = conn.createStatement();
				ResultSet rs    = stmt.executeQuery(QUERY)){

			LOG.info("**running query " + QUERY);

			// loop through the result set
			BulkRequest request;
			TimbermillBulkRequest timbermillBulkRequest;

			while (rs.next()) {
				// fetch the serialized object to a byte array
				byte[] st = (byte[])rs.getObject(2);
				request = new BulkRequest();
				request.readFrom(StreamInput.wrap(st));
				timbermillBulkRequest = new TimbermillBulkRequest(request);
				timbermillBulkRequest.setId(rs.getInt(1));
				timbermillBulkRequest.setCreateTime(rs.getDate(3));
				timbermillBulkRequest.setUpdateTime(rs.getDate(4));
				timbermillBulkRequest.setTimesFetched(rs.getInt(5));
				timbermillBulkRequest.setInProgress(rs.getBoolean(6));

				timbermillBulkRequests.add(timbermillBulkRequest);
			}
			return timbermillBulkRequests;

		} catch (SQLException e) {
			System.out.println(e.getMessage());

		} catch (IOException e) {
			e.printStackTrace();
		}
		return timbermillBulkRequests;
	}

	@Override public void persistToDisk(TimbermillBulkRequest timbermillBulkRequest) {
		BulkRequest request = timbermillBulkRequest.getRequest();
		try (Connection conn = DriverManager.getConnection(URL) ; PreparedStatement pstmt = conn.prepareStatement(INSERT)) {
			BytesStreamOutput out = new BytesStreamOutput();
			request.writeTo(out);
			//pstmt.setInt(1, timbermillBulkRequest.getId());
			pstmt.setBytes(2, out.bytes().toBytesRef().bytes);
			pstmt.setDate(3, timbermillBulkRequest.getCreateTime());
			pstmt.setDate(4, timbermillBulkRequest.getUpdateTime());
			pstmt.setInt(5, timbermillBulkRequest.getTimesFetched());
			pstmt.setBoolean(6, timbermillBulkRequest.isInProgress());
			pstmt.executeUpdate();
		} catch (Exception e) {
			LOG.error("Failed persisting bulk request to disk. Request: " + timbermillBulkRequest.getRequest().requests().toString());
			throw new RuntimeException(e);
		}
	}

}


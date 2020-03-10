package com.datorama.oss.timbermill.common;

import java.util.List;

import org.tmatesoft.sqljet.core.SqlJetException;

public interface DiskHandler {
	List<DbBulkRequest> fetchFailedBulks();

	void persistToDisk(DbBulkRequest dbBulkRequest);

	void deleteBulk(DbBulkRequest dbBulkRequest);

	void updateBulk(String id, DbBulkRequest dbBulkRequest);

	boolean hasFailedBulks();
}


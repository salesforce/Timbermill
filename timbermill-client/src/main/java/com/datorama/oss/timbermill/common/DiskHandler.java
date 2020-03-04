package com.datorama.oss.timbermill.common;

import java.util.List;

public interface DiskHandler {
	List<TimbermillBulkRequest> fetchFailedBulks();

	void persistToDisk(TimbermillBulkRequest timbermillBulkRequest);
}


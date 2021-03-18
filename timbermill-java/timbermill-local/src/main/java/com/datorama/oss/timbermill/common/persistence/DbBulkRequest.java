package com.datorama.oss.timbermill.common.persistence;

import org.elasticsearch.action.bulk.BulkRequest;

import java.io.Serializable;
import java.util.UUID;

public class DbBulkRequest implements Serializable {
	private String id = UUID.randomUUID().toString();
	private int timesFetched = 0;
	private String insertTime;
	private BulkRequest request;

	public DbBulkRequest() {
	}

	public DbBulkRequest(BulkRequest request) {
		this.request = request;
	}

	public int numOfActions(){
		return request.numberOfActions();
	}

	public String getId() {
		return id;
	}

	public DbBulkRequest setId(String id) {
		this.id = id;
		return this;
	}

	public BulkRequest getRequest() {
		return request;
	}

	public DbBulkRequest setRequest(BulkRequest request) {
		this.request = request;
		return this;
	}

	public int getTimesFetched() {
		return timesFetched;
	}

	public DbBulkRequest setTimesFetched(int timesFetched) {
		this.timesFetched = timesFetched;
		return this;
	}

	public String getInsertTime() {
		return insertTime;
	}

	public DbBulkRequest setInsertTime(String insertTime) {
		this.insertTime = insertTime;
		return this;
	}

	public long estimatedSize() {
		return request.estimatedSizeInBytes();
	}
}

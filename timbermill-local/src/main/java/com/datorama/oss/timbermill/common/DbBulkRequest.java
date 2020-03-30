package com.datorama.oss.timbermill.common;

import java.io.Serializable;

import org.elasticsearch.action.bulk.BulkRequest;

public class DbBulkRequest implements Serializable {
	private int id = -1;
	private int timesFetched = 0;
	private String insertTime;
	private BulkRequest request;


	public DbBulkRequest(BulkRequest request) {
		this.request = request;
	}

	public int size(){
		return getRequest().numberOfActions();
	}

	public int getId() {
		return id;
	}

	public DbBulkRequest setId(int id) {
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

}

package com.datorama.oss.timbermill.common;

import java.io.Serializable;

import org.elasticsearch.action.bulk.BulkRequest;
import org.joda.time.DateTime;

public class DbBulkRequest implements Serializable {
	private int id = -1;
	private BulkRequest request;
	private String createTime;
	private String insertTime;
	private int timesFetched;


	public DbBulkRequest(BulkRequest request) {
		this.request = request;
		this.createTime = DateTime.now().toString();
		this.timesFetched = 0;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public BulkRequest getRequest() {
		return request;
	}

	public void setRequest(BulkRequest request) {
		this.request = request;
	}

	public String getCreateTime() {
		return createTime;
	}

	public void setCreateTime(String createTime) {
		this.createTime = createTime;
	}

	public int getTimesFetched() {
		return timesFetched;
	}

	public void setTimesFetched(int timesFetched) {
		this.timesFetched = timesFetched;
	}

	public String getInsertTime() {
		return insertTime;
	}

	public void setInsertTime(String insertTime) {
		this.insertTime = insertTime;
	}

}

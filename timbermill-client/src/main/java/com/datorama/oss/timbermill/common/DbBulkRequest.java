package com.datorama.oss.timbermill.common;

import java.io.Serializable;
import java.util.Date;
import java.util.Objects;
import java.util.UUID;

import org.elasticsearch.action.bulk.BulkRequest;

public class DbBulkRequest implements Serializable {
	private String id;
	private BulkRequest request;
	private String createTime;
	private Long insertTime;
	private int timesFetched;


	public DbBulkRequest(BulkRequest request) {
		this.request = request;
		String uniqueID = UUID.randomUUID().toString();
		setId(uniqueID);
		setCreateTime(new Date(System.currentTimeMillis()).toString());
	}

	public String  getId() {
		return id;
	}

	public void setId(String id) {
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

	public Long getInsertTime() {
		return insertTime;
	}

	public void setInsertTime(Long insertTime) {
		this.insertTime = insertTime;
	}

	@Override public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		DbBulkRequest that = (DbBulkRequest) o;
		return id.equals(that.id) &&
				insertTime == that.insertTime &&
				Objects.equals(createTime, that.createTime);
	}
}

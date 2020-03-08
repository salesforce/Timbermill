package com.datorama.oss.timbermill.common;


import java.io.Serializable;
import java.sql.Date;
import java.util.Objects;
import java.util.UUID;

import org.elasticsearch.action.bulk.BulkRequest;

public class DbBulkRequest implements Serializable {
	private String id;
	private BulkRequest request;
	private Date createTime;
	private Long insertTime;
	private int timesFetched;
	private boolean inDisk;


	public DbBulkRequest(BulkRequest request) {
		this.request = request;
		String uniqueID = UUID.randomUUID().toString();
		setId(uniqueID);
//		setCreateTime(new Date(System.currentTimeMillis())); TODO check if we can insert Date type in db
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

	public Date getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

	public int getTimesFetched() {
		return timesFetched;
	}

	public void setTimesFetched(int timesFetched) {
		this.timesFetched = timesFetched;
	}

	public boolean isInDisk() {
		return inDisk;
	}

	public void setInDisk(boolean inDisk) {
		this.inDisk = inDisk;
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
				inDisk == that.inDisk &&
				insertTime == that.insertTime &&
				Objects.equals(createTime, that.createTime);
	}
}

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
	private Date updateTime;
	private int timesFetched;
	private boolean inDisk;


	public DbBulkRequest(BulkRequest request) {
		this.request = request;
		String uniqueID = UUID.randomUUID().toString();
		setId(uniqueID);
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

	public Date getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(Date updateTime) {
		this.updateTime = updateTime;
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

	@Override public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		DbBulkRequest that = (DbBulkRequest) o;
		return id.equals(that.id) &&
				timesFetched == that.timesFetched &&
				inDisk == that.inDisk &&
				Objects.equals(createTime, that.createTime) &&
				Objects.equals(updateTime, that.updateTime);
	}
}

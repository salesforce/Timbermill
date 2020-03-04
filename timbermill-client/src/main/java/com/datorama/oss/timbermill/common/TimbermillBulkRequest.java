package com.datorama.oss.timbermill.common;


import java.io.Serializable;
import java.sql.Date;
import java.util.Objects;
import java.util.UUID;

import org.elasticsearch.action.bulk.BulkRequest;

public class TimbermillBulkRequest implements Serializable {
	private int id;
	private BulkRequest request;
	private Date createTime;
	private Date updateTime;
	private int timesFetched;
	private boolean inProgress;

	public TimbermillBulkRequest(BulkRequest request) {
		this.request = request;
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

	public boolean isInProgress() {
		return inProgress;
	}

	public void setInProgress(boolean inProgress) {
		this.inProgress = inProgress;
	}

	@Override public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		TimbermillBulkRequest that = (TimbermillBulkRequest) o;
		return id == that.id &&
				timesFetched == that.timesFetched &&
				inProgress == that.inProgress &&
				Objects.equals(createTime, that.createTime) &&
				Objects.equals(updateTime, that.updateTime);
	}
}

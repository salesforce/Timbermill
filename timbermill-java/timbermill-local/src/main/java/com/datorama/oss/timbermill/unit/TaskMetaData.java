package com.datorama.oss.timbermill.unit;

import java.time.ZonedDateTime;

public class TaskMetaData {
    private ZonedDateTime taskBegin;
    private ZonedDateTime taskEnd;
    private Long duration;
	private ZonedDateTime dateToDelete;

	ZonedDateTime getTaskBegin() {
        return taskBegin;
    }

    void setTaskBegin(ZonedDateTime taskBegin) {
        this.taskBegin = taskBegin;
    }

    ZonedDateTime getTaskEnd() {
        return taskEnd;
    }

    void setTaskEnd(ZonedDateTime taskEnd) {
        this.taskEnd = taskEnd;
    }

    void setDuration(Long duration) {
        this.duration = duration;
    }

    Long getDuration() {
        return duration;
    }

	ZonedDateTime getDateToDelete() {
		return dateToDelete;
	}

	void setDateToDelete(ZonedDateTime dateToDelete) {
		this.dateToDelete = dateToDelete;
	}

	@Override public String toString() {
		return "TaskMetaData{" +
				"taskBegin=" + taskBegin +
				", taskEnd=" + taskEnd +
				", duration=" + duration +
				", dateToDelete=" + dateToDelete +
				'}';
	}
}

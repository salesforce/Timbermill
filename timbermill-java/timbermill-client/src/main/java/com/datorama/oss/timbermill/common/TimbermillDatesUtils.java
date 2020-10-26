package com.datorama.oss.timbermill.common;

import java.time.ZonedDateTime;

public class TimbermillDatesUtils {

	public static ZonedDateTime getDateToDeleteWithDefault(long defaultDaysRotation) {
		return getDateToDeleteWithDefault(defaultDaysRotation, null);
	}

	public static ZonedDateTime getDateToDeleteWithDefault(long defaultDaysRotation, ZonedDateTime dateToDelete) {
		if (dateToDelete == null){
			dateToDelete = ZonedDateTime.now();
			if (defaultDaysRotation > 0){
				dateToDelete = dateToDelete.plusDays(defaultDaysRotation);
			}
		}
		dateToDelete = dateToDelete.withHour(0).withMinute(0).withSecond(0).withNano(0);
		return dateToDelete;
	}
}

package com.datorama.timbermill.common;

import java.time.ZonedDateTime;

import com.datorama.timbermill.ElasticsearchClient;
import com.datorama.timbermill.ElasticsearchParams;
import com.datorama.timbermill.TaskIndexer;

import static com.datorama.timbermill.common.Constants.INDEX_DELIMITER;
import static com.datorama.timbermill.common.Constants.TIMBERMILL_INDEX_PREFIX;

public class TimbermillUtils {
	public static TaskIndexer bootstrap(ElasticsearchParams elasticsearchParams, ElasticsearchClient es) {
//		es.bootstrapElasticsearch(elasticsearchParams.getNumberOfShards(), elasticsearchParams.getNumberOfReplicas()); //todo change
		es.runDeletionTaskCron();
		return new TaskIndexer(elasticsearchParams, es);
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

	public static ZonedDateTime getDefaultDateToDelete(long defaultDaysRotation) {
		return getDateToDeleteWithDefault(defaultDaysRotation, null);
	}

	public static String getTimbermillIndexAlias(String env) {
		return TIMBERMILL_INDEX_PREFIX + INDEX_DELIMITER + env;
	}
}

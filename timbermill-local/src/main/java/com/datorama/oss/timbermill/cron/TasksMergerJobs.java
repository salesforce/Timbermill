package com.datorama.oss.timbermill.cron;

import java.util.Map;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.oss.timbermill.ElasticsearchClient;
import com.datorama.oss.timbermill.common.ElasticsearchUtil;
import com.datorama.oss.timbermill.unit.Task;
import com.google.common.collect.Maps;

import static com.datorama.oss.timbermill.ElasticsearchClient.ALL_TASK_FIELDS;
import static com.datorama.oss.timbermill.ElasticsearchClient.PARTIALS_QUERY;
import static org.elasticsearch.common.Strings.EMPTY_ARRAY;

public class TasksMergerJobs implements Job {

	private static final Logger LOG = LoggerFactory.getLogger(TasksMergerJobs.class);
	private ElasticsearchClient client;

	@Override public void execute(JobExecutionContext context) {
		client = (ElasticsearchClient) context.getJobDetail().getJobDataMap().get(ElasticsearchUtil.CLIENT);
		String currentIndex = client.getCurrentIndex();
		String previousIndex = client.getOldIndex();
		if (indexExists(previousIndex)){
			LOG.info("About to merge partial tasks between indices");
			int size = migrateMatchingTasksToNewIndexPartialTasks(currentIndex, previousIndex);
			LOG.info("Finished merging {} partial tasks.", size);
		}
	}

	private boolean indexExists(String index) {
		return index != null;
	}

	private int migrateMatchingTasksToNewIndexPartialTasks(String currentIndex, String previousIndex) {
		Map<String, Task> previousIndexMatchingTasks = Maps.newHashMap();
		String functionDescription = "Migrate old tasks to new index'es partial tasks";
		Map<String, Task> singleTaskByIds = client.getSingleTaskByIds(PARTIALS_QUERY, currentIndex, functionDescription, EMPTY_ARRAY, ALL_TASK_FIELDS);
		if (!singleTaskByIds.isEmpty()) {
			previousIndexMatchingTasks = client.getTasksByIds(previousIndex, singleTaskByIds.keySet(), functionDescription, ALL_TASK_FIELDS, EMPTY_ARRAY);
			client.indexAndDeleteTasks(previousIndexMatchingTasks);
		}
		return previousIndexMatchingTasks.size();
	}
}

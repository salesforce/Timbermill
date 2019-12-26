package com.datorama.timbermill.cron;

import java.io.IOException;
import java.util.Map;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.timbermill.ElasticsearchClient;
import com.datorama.timbermill.common.TimbermillUtils;
import com.datorama.timbermill.unit.Task;

public class TasksMergerJobs implements Job {

	private static final Logger LOG = LoggerFactory.getLogger(TasksMergerJobs.class);
	private ElasticsearchClient client;

	@Override public void execute(JobExecutionContext context) {
		LOG.info("About to merge partial tasks between indices");
		client = (ElasticsearchClient) context.getJobDetail().getJobDataMap().get(TimbermillUtils.CLIENT);
		String currentIndex = client.getCurrentIndex();
		String previousIndex = client.getOldIndex();
		if (indexExists(previousIndex)){
			try {
				migrateOldPartialTaskToNewIndex(currentIndex, previousIndex);
				LOG.info("Finished merging partial tasks.");
			} catch (IOException e) {
				LOG.error("Could not merge partial tasks in indices [{}] [{}]", previousIndex, currentIndex);
			}
		}
		else{
			LOG.error("Old index doesn't exists, will not merge partial tasks");
		}
	}

	private boolean indexExists(String index) {
		return index != null;
	}

	private void migrateOldPartialTaskToNewIndex(String currentIndex, String previousIndex) throws IOException {
		Map<String, Task> currentIndexPartialTasks = client.getIndexPartialTasks(currentIndex);
		Map<String, Task> previousIndexMatchingTasks = client.fetchTasksByIdsFromIndex(previousIndex, currentIndexPartialTasks.keySet());
		client.indexAndDeleteTasks(previousIndexMatchingTasks);
	}
}

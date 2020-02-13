package com.datorama.oss.timbermill.cron;

import java.io.IOException;
import java.util.Map;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.oss.timbermill.ElasticsearchClient;
import com.datorama.oss.timbermill.common.ElasticsearchUtil;
import com.datorama.oss.timbermill.unit.Task;
import com.google.common.collect.Maps;

public class TasksMergerJobs implements Job {

	private static final Logger LOG = LoggerFactory.getLogger(TasksMergerJobs.class);
	private ElasticsearchClient client;

	@Override public void execute(JobExecutionContext context) {
		client = (ElasticsearchClient) context.getJobDetail().getJobDataMap().get(ElasticsearchUtil.CLIENT);
		String currentIndex = client.getCurrentIndex();
		String previousIndex = client.getOldIndex();
		if (indexExists(previousIndex)){
			LOG.info("About to merge partial tasks between indices");
			try {
				int size = migrateOldPartialTaskToNewIndex(currentIndex, previousIndex);
				LOG.info("Finished merging {} partial tasks.", size);
			} catch (IOException e) {
				LOG.error("Could not merge partial tasks between indices [{}] [{}]", previousIndex, currentIndex);
			}
		}
	}

	private boolean indexExists(String index) {
		return index != null;
	}

	private int migrateOldPartialTaskToNewIndex(String currentIndex, String previousIndex) throws IOException {
		Map<String, Task> previousIndexMatchingTasks = Maps.newHashMap();
		Map<String, Task> currentIndexPartialTasks = client.getIndexPartialTasks(currentIndex);
		if (!currentIndexPartialTasks.isEmpty()) {
			previousIndexMatchingTasks = client.fetchTasksByIdsFromIndex(previousIndex, currentIndexPartialTasks.keySet());
			if (!previousIndexMatchingTasks.isEmpty()) {
				client.indexAndDeleteTasks(previousIndexMatchingTasks);
			}
		}
		return previousIndexMatchingTasks.size();
	}
}

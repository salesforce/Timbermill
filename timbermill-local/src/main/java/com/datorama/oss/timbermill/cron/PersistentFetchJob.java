package com.datorama.oss.timbermill.cron;

import org.quartz.Job;
import org.quartz.JobExecutionContext;

import com.datorama.oss.timbermill.common.DiskHandler;
import com.datorama.oss.timbermill.common.ElasticsearchUtil;

public class PersistentFetchJob implements Job {

	@Override public void execute(JobExecutionContext context) {
		DiskHandler diskHandler = (DiskHandler) context.getJobDetail().getJobDataMap().get(ElasticsearchUtil.DISK_HANDLER);
		diskHandler.fetchFailedBulks();
	}
}


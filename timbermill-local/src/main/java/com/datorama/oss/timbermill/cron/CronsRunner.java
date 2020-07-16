package com.datorama.oss.timbermill.cron;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.common.Strings;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.oss.timbermill.ElasticsearchClient;
import com.datorama.oss.timbermill.common.disk.DiskHandler;
import com.datorama.oss.timbermill.unit.Event;

import static com.datorama.oss.timbermill.common.ElasticsearchUtil.*;
import static com.datorama.oss.timbermill.common.ElasticsearchUtil.OVERFLOWED_EVENTS_QUEUE;
import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

public class CronsRunner {

	private static final Logger LOG = LoggerFactory.getLogger(CronsRunner.class);

	public static void runCrons(String bulkPersistentFetchCronExp, String eventsPersistentFetchCronExp, DiskHandler diskHandler, ElasticsearchClient es, String deletionCronExp, BlockingQueue<Event> buffer,
			BlockingQueue<Event> overFlowedEvents) {
		if (diskHandler != null) {
			if (!Strings.isEmpty(bulkPersistentFetchCronExp)) {
				runBulkPersistentFetchCron(bulkPersistentFetchCronExp, es, diskHandler);
			}

			if (!Strings.isEmpty(bulkPersistentFetchCronExp)) {
				runEventsPersistentFetchCron(eventsPersistentFetchCronExp, diskHandler, buffer, overFlowedEvents);
			}
		}
		if (!Strings.isEmpty(deletionCronExp)) {
			runDeletionTaskCron(deletionCronExp, es);
		}
	}

	private static void runEventsPersistentFetchCron(String eventsPersistentFetchCronExp, DiskHandler diskHandler, BlockingQueue<Event> buffer, BlockingQueue<Event> overFlowedEvents) {
		try {
			final StdSchedulerFactory sf = new StdSchedulerFactory();
			Scheduler scheduler = sf.getScheduler();
			JobDataMap jobDataMap = new JobDataMap();
			jobDataMap.put(DISK_HANDLER, diskHandler);
			jobDataMap.put(EVENTS_QUEUE, buffer);
			jobDataMap.put(OVERFLOWED_EVENTS_QUEUE, overFlowedEvents);

			JobDetail job = newJob(EventsPersistentFetchJob.class)
					.withIdentity("job3", "group3").usingJobData(jobDataMap)
					.build();
			CronTrigger trigger = newTrigger()
					.withIdentity("trigger3", "group3")
					.withSchedule(cronSchedule(eventsPersistentFetchCronExp))
					.build();

			scheduler.scheduleJob(job, trigger);
			scheduler.start();
		} catch (SchedulerException e) {
			LOG.error("Error occurred while fetching failed bulks from disk", e);
		}
	}

	private static void runBulkPersistentFetchCron(String bulkPersistentFetchCronExp, ElasticsearchClient es, DiskHandler diskHandler) {
		try {
			final StdSchedulerFactory sf = new StdSchedulerFactory();
			Scheduler scheduler = sf.getScheduler();
			JobDataMap jobDataMap = new JobDataMap();
			jobDataMap.put(CLIENT, es);
			jobDataMap.put(DISK_HANDLER, diskHandler);
			JobDetail job = newJob(BulkPersistentFetchJob.class)
					.withIdentity("job2", "group2").usingJobData(jobDataMap)
					.build();
			CronTrigger trigger = newTrigger()
					.withIdentity("trigger2", "group2")
					.withSchedule(cronSchedule(bulkPersistentFetchCronExp))
					.build();

			scheduler.scheduleJob(job, trigger);
			scheduler.start();
		} catch (SchedulerException e) {
			LOG.error("Error occurred while fetching failed bulks from disk", e);
		}
	}

	private static void runDeletionTaskCron(String deletionCronExp, ElasticsearchClient es) {
		try {
			final StdSchedulerFactory sf = new StdSchedulerFactory();
			Scheduler scheduler = sf.getScheduler();
			JobDataMap jobDataMap = new JobDataMap();
			jobDataMap.put(CLIENT, es);
			JobDetail job = newJob(ExpiredTasksDeletionJob.class)
					.withIdentity("job1", "group1").usingJobData(jobDataMap)
					.build();
			CronTrigger trigger = newTrigger()
					.withIdentity("trigger1", "group1")
					.withSchedule(cronSchedule(deletionCronExp))
					.build();

			scheduler.scheduleJob(job, trigger);
			scheduler.start();
		} catch (SchedulerException e) {
			LOG.error("Error occurred while deleting expired tasks", e);
		}

	}

}

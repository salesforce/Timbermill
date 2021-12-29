package com.datorama.oss.timbermill.cron;

import java.util.concurrent.BlockingQueue;

import com.datorama.oss.timbermill.common.redis.RedisService;
import com.google.common.cache.LoadingCache;
import io.github.resilience4j.ratelimiter.RateLimiter;
import org.elasticsearch.common.Strings;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.oss.timbermill.ElasticsearchClient;
import com.datorama.oss.timbermill.common.persistence.PersistenceHandler;
import com.datorama.oss.timbermill.unit.Event;

import static com.datorama.oss.timbermill.common.ElasticsearchUtil.*;
import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

public class CronsRunner {

	private static final Logger LOG = LoggerFactory.getLogger(CronsRunner.class);
	private Scheduler scheduler;

	public void runCrons(String bulkPersistentFetchCronExp, String eventsPersistentFetchCronExp, PersistenceHandler persistenceHandler, ElasticsearchClient es, String deletionCronExp, BlockingQueue<Event> buffer,
						 BlockingQueue<Event> overFlowedEvents, String mergingCronExp, RedisService redisService, LoadingCache<String, RateLimiter> rateLimiterMap) {
		final StdSchedulerFactory sf = new StdSchedulerFactory();
		try {
			 scheduler = sf.getScheduler();
			if (persistenceHandler != null) {
				if (!Strings.isEmpty(bulkPersistentFetchCronExp)) {
					runBulkPersistentFetchCron(bulkPersistentFetchCronExp, es, persistenceHandler, rateLimiterMap);
				}

				if (!Strings.isEmpty(eventsPersistentFetchCronExp)) {
					runEventsPersistentFetchCron(eventsPersistentFetchCronExp, persistenceHandler, buffer, overFlowedEvents);
				}
			}
			if (!Strings.isEmpty(deletionCronExp)) {
				runDeletionTaskCron(deletionCronExp, es, redisService);
			}
			if (!Strings.isEmpty(mergingCronExp)) {
				runPartialMergingTasksCron(es, mergingCronExp, redisService);
			}
			scheduler.start();
		} catch (SchedulerException e) {
			LOG.error("Could not start crons", e);
			throw new RuntimeException(e);
		}
	}

	public void close(){
		try {
			scheduler.shutdown();
		} catch (SchedulerException e) {
			LOG.error("Could not close scheduler", e);
		}
	}

	private void runEventsPersistentFetchCron(String eventsPersistentFetchCronExp, PersistenceHandler persistenceHandler, BlockingQueue<Event> buffer,
											  BlockingQueue<Event> overFlowedEvents) throws SchedulerException {
		JobDataMap jobDataMap = new JobDataMap();
		jobDataMap.put(PERSISTENCE_HANDLER, persistenceHandler);
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
	}

	private void runBulkPersistentFetchCron(String bulkPersistentFetchCronExp, ElasticsearchClient es, PersistenceHandler persistenceHandler, LoadingCache<String, RateLimiter> rateLimiterMap) throws SchedulerException {
		JobDataMap jobDataMap = new JobDataMap();
		jobDataMap.put(CLIENT, es);
		jobDataMap.put(PERSISTENCE_HANDLER, persistenceHandler);
		jobDataMap.put(RATE_LIMITER_MAP, rateLimiterMap);
		JobDetail job = newJob(BulkPersistentFetchJob.class)
				.withIdentity("job2", "group2").usingJobData(jobDataMap)
				.build();
		CronTrigger trigger = newTrigger()
				.withIdentity("trigger2", "group2")
				.withSchedule(cronSchedule(bulkPersistentFetchCronExp))
				.build();
		scheduler.scheduleJob(job, trigger);
	}

	private void runDeletionTaskCron(String deletionCronExp, ElasticsearchClient es, RedisService redisService) throws SchedulerException {
		JobDataMap jobDataMap = new JobDataMap();
		jobDataMap.put(CLIENT, es);
		jobDataMap.put(REDIS_SERVICE, redisService);
		JobDetail job = newJob(ExpiredTasksDeletionJob.class)
				.withIdentity("job1", "group1").usingJobData(jobDataMap)
				.build();
		CronTrigger trigger = newTrigger()
				.withIdentity("trigger1", "group1")
				.withSchedule(cronSchedule(deletionCronExp))
				.build();

		scheduler.scheduleJob(job, trigger);
	}

	private void runPartialMergingTasksCron(ElasticsearchClient es, String mergingCronExp, RedisService redisService) throws SchedulerException{
		JobDataMap jobDataMap = new JobDataMap();
		jobDataMap.put(CLIENT, es);
		jobDataMap.put(REDIS_SERVICE, redisService);
		JobDetail job = newJob(TasksMergerJobs.class)
				.withIdentity("job5", "group5").usingJobData(jobDataMap)
				.build();
		CronTrigger trigger = newTrigger()
				.withIdentity("trigger5", "group5")
				.withSchedule(cronSchedule(mergingCronExp))
				.build();
		scheduler.scheduleJob(job, trigger);
	}

}

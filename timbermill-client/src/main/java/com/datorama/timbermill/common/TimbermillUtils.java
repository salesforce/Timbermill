package com.datorama.timbermill.common;

import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

import org.elasticsearch.common.Strings;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.timbermill.ElasticsearchClient;
import com.datorama.timbermill.ElasticsearchParams;
import com.datorama.timbermill.TaskIndexer;
import com.datorama.timbermill.cron.ExpiredTasksDeletionJob;
import com.datorama.timbermill.cron.TasksMergerJobs;
import com.datorama.timbermill.unit.Event;
import com.google.common.collect.Sets;

import static com.datorama.timbermill.common.Constants.*;
import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

public class TimbermillUtils {
	public static final String CLIENT = "client";
	public static final int THREAD_SLEEP = 2000;
	private static final int MAX_ELEMENTS = 100000;
	private static final Logger LOG = LoggerFactory.getLogger(TimbermillUtils.class);
	private static String mergingCronExp;
	private static Set<String> envsSet = Sets.newConcurrentHashSet();

	public static TaskIndexer bootstrap(ElasticsearchParams elasticsearchParams, ElasticsearchClient es) {
		// es.bootstrapElasticsearch(elasticsearchParams.getNumberOfShards(), elasticsearchParams.getNumberOfReplicas()); todo fix
		mergingCronExp = elasticsearchParams.getMergingCronExp();

		String deletionCronExp = elasticsearchParams.getDeletionCronExp();
		if (!Strings.isEmpty(deletionCronExp)) {
			runDeletionTaskCron(deletionCronExp, es);
		}
		return new TaskIndexer(elasticsearchParams, es);
	}

	public static void drainAndIndex(BlockingQueue<Event> eventsQueue, TaskIndexer taskIndexer, ElasticsearchClient es) {
		while (!eventsQueue.isEmpty()) {
			try {
				es.retryFailedRequests();

				Collection<Event> events = new ArrayList<>();
				eventsQueue.drainTo(events, MAX_ELEMENTS);
				Map<String, List<Event>> eventsPerEnvMap = events.stream().collect(Collectors.groupingBy(Event::getEnv));
				for (Map.Entry<String, List<Event>> eventsPerEnv : eventsPerEnvMap.entrySet()) {
					String env = eventsPerEnv.getKey();
					if (!envsSet.contains(env)) {
						envsSet.add(env);
						runPartialMergingTasksCron(env, es);
					}

					Collection<Event> currentEvents = eventsPerEnv.getValue();
					taskIndexer.retrieveAndIndex(currentEvents, env);
				}
			} catch (RuntimeException e) {
				LOG.error("Error was thrown from TaskIndexer:", e);
			}
		}
		try {
			Thread.sleep(THREAD_SLEEP);
		} catch (InterruptedException e) {
			LOG.error("Error was thrown from TaskIndexer:", e);
		}
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

	public static String getIndexSerial(int serialNumber) {
		return String.format("%06d", serialNumber);
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

	private static void runPartialMergingTasksCron(String env, ElasticsearchClient es) {
		try {
			final StdSchedulerFactory sf = new StdSchedulerFactory();
			Scheduler scheduler = sf.getScheduler();
			JobDataMap jobDataMap = new JobDataMap();
			jobDataMap.put(CLIENT, es);
			JobDetail job = newJob(TasksMergerJobs.class)
					.withIdentity("job" + env, "group" + env).usingJobData(jobDataMap)
					.build();
			CronTrigger trigger = newTrigger()
					.withIdentity("trigger" + env, "group" + env)
					.withSchedule(cronSchedule(mergingCronExp))
					.build();

			scheduler.scheduleJob(job, trigger);
			scheduler.start();
		} catch (SchedulerException e) {
			LOG.error("Error occurred while merging partial tasks", e);
		}

	}
}

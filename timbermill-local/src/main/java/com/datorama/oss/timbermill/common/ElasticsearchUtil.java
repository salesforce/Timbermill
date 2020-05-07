package com.datorama.oss.timbermill.common;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

import org.elasticsearch.common.Strings;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.oss.timbermill.ElasticsearchClient;
import com.datorama.oss.timbermill.ElasticsearchParams;
import com.datorama.oss.timbermill.TaskIndexer;
import com.datorama.oss.timbermill.cron.ExpiredTasksDeletionJob;
import com.datorama.oss.timbermill.cron.PersistentFetchJob;
import com.datorama.oss.timbermill.cron.TasksMergerJobs;
import com.datorama.oss.timbermill.unit.Event;
import com.google.common.collect.Sets;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

public class ElasticsearchUtil {
	public static final String CLIENT = "client";
	public static final int THREAD_SLEEP = 2000;
	public static final String SCRIPT =
			"if (params.orphan != null && !params.orphan) {"
					+ "            ctx._source.orphan = false;        "
					+ "}        "
					+ "if (params.dateToDelete != null) {"
					+ "            ctx._source.meta.dateToDelete = params.dateToDelete;        "
					+ "}        "
					+ "if (params.status != null){"
					+ "        if (ctx._source.string == null){"
					+ "                ctx._source.string =  new HashMap();"
					+ "        }"
					+ "        if (params.status.equals( \\\"CORRUPTED\\\")){"
					+ "                ctx._source.status =  \\\"CORRUPTED\\\" ;"
					+ "        }"
					+ "        if (ctx._source.status.equals( \\\"SUCCESS\\\" ) || ctx._source.status.equals( \\\"ERROR\\\" )){"
					+ "            if(params.status.equals( \\\"SUCCESS\\\" ) || params.status.equals( \\\"ERROR\\\" )){"
					+ "                ctx._source.status =  \\\"CORRUPTED\\\" ;"
					+ "                ctx._source.string.put(\\\"corruptedReason\\\",\\\"ALREADY_CLOSED\\\");"
					+ "            }"
					+ "            else if (params.status.equals( \\\"UNTERMINATED\\\")){"
					+ "                ctx._source.status =  \\\"CORRUPTED\\\" ;"
					+ "                ctx._source.string.put(\\\"corruptedReason\\\",\\\"ALREADY_STARTED\\\");"
					+ "            }"
					+ "            else if (params.status.equals( \\\"PARTIAL_SUCCESS\\\")){"
					+ "                ctx._source.status =  \\\"CORRUPTED\\\" ;"
					+ "                ctx._source.string.put(\\\"corruptedReason\\\",\\\"ALREADY_CLOSED\\\");"
					+ "            }"
					+ "            else if (params.status.equals( \\\"PARTIAL_ERROR\\\")){"
					+ "                ctx._source.status =  \\\"CORRUPTED\\\" ;"
					+ "                ctx._source.string.put(\\\"corruptedReason\\\",\\\"ALREADY_CLOSED\\\");"
					+ "            }"
					+ "        }"
					+ "        else if (ctx._source.status.equals( \\\"UNTERMINATED\\\")){"
					+ "            if(params.status.equals( \\\"SUCCESS\\\" ) || params.status.equals( \\\"ERROR\\\" )){"
					+ "                ctx._source.status =  \\\"CORRUPTED\\\" ;"
					+ "                ctx._source.string.put(\\\"corruptedReason\\\",\\\"ALREADY_STARTED\\\");"
					+ "            }"
					+ "            else if (params.status.equals( \\\"UNTERMINATED\\\")){"
					+ "                ctx._source.status =  \\\"CORRUPTED\\\" ;"
					+ "                ctx._source.string.put(\\\"corruptedReason\\\",\\\"ALREADY_STARTED\\\");"
					+ "            }"
					+ "            else if (params.status.equals( \\\"PARTIAL_SUCCESS\\\")){"
					+ "                long taskBegin = ZonedDateTime.parse(ctx._source.meta.taskBegin, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant().toEpochMilli();"
					+ "                ctx._source.meta.duration = params.taskEndMillis - taskBegin;"
					+ "                ctx._source.meta.taskEnd = params.taskEnd;"
					+ "                ctx._source.status =  \\\"SUCCESS\\\" ;"
					+ "            }"
					+ "            else if (params.status.equals( \\\"PARTIAL_ERROR\\\")){"
					+ "                long taskBegin = ZonedDateTime.parse(ctx._source.meta.taskBegin, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant().toEpochMilli();"
					+ "                ctx._source.meta.duration = params.taskEndMillis - taskBegin;"
					+ "                ctx._source.meta.taskEnd = params.taskEnd;"
					+ "                ctx._source.status = \\\"ERROR\\\";"
					+ "            }"
					+ "        }"
					+ "        else if (ctx._source.status.equals( \\\"PARTIAL_SUCCESS\\\")){"
					+ "            if(params.status.equals( \\\"SUCCESS\\\" ) || params.status.equals( \\\"ERROR\\\" )){"
					+ "                ctx._source.status =  \\\"CORRUPTED\\\" ;"
					+ "                ctx._source.string.put(\\\"corruptedReason\\\",\\\"ALREADY_CLOSED\\\");"
					+ "            }"
					+ "            if (params.status.equals( \\\"UNTERMINATED\\\")){"
					+ "                long taskEnd = ZonedDateTime.parse(ctx._source.meta.taskEnd, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant().toEpochMilli();"
					+ "                ctx._source.meta.duration = taskEnd - params.taskBeginMillis;"
					+ "                ctx._source.meta.taskBegin = params.taskBegin;"
					+ "                ctx._source.status =  \\\"SUCCESS\\\" ;"
					+ "            }"
					+ "            else if (params.status.equals( \\\"PARTIAL_SUCCESS\\\")){"
					+ "                ctx._source.status =  \\\"CORRUPTED\\\" ;"
					+ "                ctx._source.string.put(\\\"corruptedReason\\\",\\\"ALREADY_CLOSED\\\");"
					+ "            }"
					+ "            else if (params.status.equals( \\\"PARTIAL_ERROR\\\")){"
					+ "                ctx._source.status =  \\\"CORRUPTED\\\" ;"
					+ "                ctx._source.string.put(\\\"corruptedReason\\\",\\\"ALREADY_CLOSED\\\");"
					+ "            }"
					+ "        }"
					+ "        else if (ctx._source.status.equals( \\\"PARTIAL_ERROR\\\")){"
					+ "            if(params.status.equals( \\\"SUCCESS\\\" ) || params.status.equals( \\\"ERROR\\\" )){"
					+ "                ctx._source.status =  \\\"CORRUPTED\\\" ;"
					+ "                ctx._source.string.put(\\\"corruptedReason\\\",\\\"ALREADY_CLOSED\\\");"
					+ "            }"
					+ "            else if (params.status.equals( \\\"UNTERMINATED\\\")){"
					+ "                long taskEnd = ZonedDateTime.parse(ctx._source.meta.taskEnd, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant().toEpochMilli();"
					+ "                ctx._source.meta.duration = taskEnd - params.taskBeginMillis;"
					+ "                ctx._source.meta.taskBegin = params.taskBegin;"
					+ "                ctx._source.status =  \\\"ERROR\\\" ;"
					+ "            }"
					+ "            else if (params.status.equals( \\\"PARTIAL_SUCCESS\\\")){"
					+ "                ctx._source.status =  \\\"CORRUPTED\\\" ;"
					+ "                ctx._source.string.put(\\\"corruptedReason\\\",\\\"ALREADY_CLOSED\\\");"
					+ "            }"
					+ "            else if (params.status.equals( \\\"PARTIAL_ERROR\\\")){"
					+ "                ctx._source.status =  \\\"CORRUPTED\\\" ;"
					+ "                ctx._source.string.put(\\\"corruptedReason\\\",\\\"ALREADY_CLOSED\\\");"
					+ "            }"
					+ "            else if (params.status.equals( \\\"CORRUPTED\\\")){"
					+ "                ctx._source.status =  \\\"CORRUPTED\\\" ;"
					+ "            }"
					+ "        }"
					+ "        else if (ctx._source.status.equals( \\\"PARTIAL_INFO_ONLY\\\")){"
					+ "            if(params.status.equals( \\\"SUCCESS\\\" ) || params.status.equals( \\\"ERROR\\\" )){"
					+ "                ctx._source.meta.duration = params.taskEndMillis - params.taskBeginMillis;"
					+ "                ctx._source.meta.taskEnd = params.taskEnd;"
					+ "                ctx._source.meta.taskBegin = params.taskBegin;"
					+ "                ctx._source.status = params.status;"
					+ "            }"
					+ "            else if (params.status.equals( \\\"UNTERMINATED\\\")){"
					+ "                ctx._source.meta.taskBegin = params.taskBegin;"
					+ "                ctx._source.status = params.status;"
					+ "            }"
					+ "            else if (params.status.equals( \\\"PARTIAL_SUCCESS\\\")){"
					+ "                ctx._source.meta.taskEnd = params.taskEnd;"
					+ "                ctx._source.status = params.status;"
					+ "            }"
					+ "            else if (params.status.equals( \\\"PARTIAL_ERROR\\\")){"
					+ "                ctx._source.meta.taskEnd = params.taskEnd;"
					+ "                ctx._source.status = params.status;"
					+ "            }"
					+ "        }"
					+ "        else {"
					+ "            ctx._source.status =  \\\"CORRUPTED\\\" ;"
					+ "        }"
					+ "        }"
					+ "        if (params.contx != null) {"
					+ "            if (ctx._source.ctx == null) {"
					+ "                ctx._source.ctx = params.contx;"
					+ "            }"
					+ "            else {"
					+ "                ctx._source.ctx.putAll(params.contx);"
					+ "            }"
					+ "        }"
					+ "        if (params.string != null) {"
					+ "            if (ctx._source.string == null) {"
					+ "                ctx._source.string = params.string;"
					+ "            }"
					+ "            else {"
					+ "                ctx._source.string.putAll(params.string);"
					+ "            }"
					+ "        }"
					+ "        if (params.text != null) {"
					+ "            if (ctx._source.text == null) {"
					+ "                ctx._source.text = params.text;"
					+ "            }"
					+ "            else {"
					+ "                ctx._source.text.putAll(params.text);"
					+ "            }"
					+ "        }"
					+ "        if (params.metric != null) {"
					+ "            if (ctx._source.metric == null) {"
					+ "                ctx._source.metric = params.metric;"
					+ "            }"
					+ "            else {"
					+ "                ctx._source.metric.putAll(params.metric);"
					+ "            }"
					+ "        }"
					+ "        if (params.logi != null) {"
					+ "            if (ctx._source.log == null) {"
					+ "                ctx._source.log = params.logi;"
					+ "            } else {"
					+ "                ctx._source.log += '\\n' + params.logi;"
					+ "            }"
					+ "        }"
					+ "        if (params.name != null) {"
					+ "            ctx._source.name = params.name;"
					+ "        }"
					+ "        if (params.parentId != null) {"
					+ "            ctx._source.parentId = params.parentId;"
					+ "        }"
					+ "        if (params.primaryId != null) {"
					+ "            ctx._source.primaryId = params.primaryId;"
					+ "        }"
					+ "        if (params.primary != null && ctx._source.primary == null) {"
					+ "            ctx._source.primary = params.primary;"
					+ "        }"
					+ "        if (params.parentsPath != null) {"
					+ "            ctx._source.parentsPath = params.parentsPath;"
					+ "        }"
					+ "        if (params.orphan != null && params.orphan) {"
					+ "            ctx._source.orphan = true;"
					+ "        }\"\n";

	public static final String MAPPING = "   {\"dynamic_templates\": [\n"
			+ "      {\n"
			+ "        \"env\": {\n"
			+ "          \"path_match\":   \"env\",\n"
			+ "          \"mapping\": {\n"
			+ "            \"type\":       \"keyword\"\n"
			+ "          }\n"
			+ "        }\n"
			+ "      },\n"
			+ "            {\n"
			+ "        \"name\": {\n"
			+ "          \"path_match\":   \"name\",\n"
			+ "          \"mapping\": {\n"
			+ "            \"type\":       \"keyword\"\n"
			+ "          }\n"
			+ "        }\n"
			+ "      },\n"
			+ "            {\n"
			+ "        \"status\": {\n"
			+ "          \"path_match\":   \"status\",\n"
			+ "          \"mapping\": {\n"
			+ "            \"type\":       \"keyword\"\n"
			+ "          }\n"
			+ "        }\n"
			+ "      },\n"
			+ "            {\n"
			+ "        \"parentId\": {\n"
			+ "          \"path_match\":   \"parentId\",\n"
			+ "          \"mapping\": {\n"
			+ "            \"type\":       \"keyword\"\n"
			+ "          }\n"
			+ "        }\n"
			+ "      },\n"
			+ "            {\n"
			+ "        \"primaryId\": {\n"
			+ "          \"path_match\":   \"primaryId\",\n"
			+ "          \"mapping\": {\n"
			+ "            \"type\":       \"keyword\"\n"
			+ "          }\n"
			+ "        }\n"
			+ "      },\n"
			+ "                  {\n"
			+ "        \"log\": {\n"
			+ "          \"path_match\":   \"log\",\n"
			+ "          \"mapping\": {\n"
			+ "            \"type\":       \"text\"\n"
			+ "          }\n"
			+ "        }\n"
			+ "      },\n"
			+ "      {\n"
			+ "        \"context\": {\n"
			+ "          \"path_match\":   \"ctx.*\",\n"
			+ "          \"mapping\": {\n"
			+ "            \"type\":       \"keyword\"\n"
			+ "          }\n"
			+ "        }\n"
			+ "      },\n"
			+ "      {\n"
			+ "        \"string\": {\n"
			+ "          \"path_match\":   \"string.*\",\n"
			+ "          \"mapping\": {\n"
			+ "            \"type\":       \"keyword\"\n"
			+ "          }\n"
			+ "        }\n"
			+ "      },\n"
			+ "      {\n"
			+ "        \"text\": {\n"
			+ "          \"path_match\":   \"text.*\",\n"
			+ "          \"mapping\": {\n"
			+ "            \"type\":       \"text\"\n"
			+ "          }\n"
			+ "        }\n"
			+ "      },\n"
			+ "            {\n"
			+ "        \"metric\": {\n"
			+ "          \"path_match\":   \"metric.*\",\n"
			+ "          \"mapping\": {\n"
			+ "            \"type\":       \"double\"\n"
			+ "          }\n"
			+ "        }\n"
			+ "      }\n"
			+ "    ]\n"
			+ "  }\n"
			+ "}";
	private static final int MAX_ELEMENTS = 100000;
	public static final String INDEX_DELIMITER = "-";

	private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchUtil.class);
	public static final String TIMBERMILL_INDEX_PREFIX = "timbermill2";

	private static String mergingCronExp;
	private static Set<String> envsSet = Sets.newConcurrentHashSet();
	public static final String ELASTIC_SEARCH_CLIENT = "elastic_search_client";
	private static final String SQLITE = "sqlite";

	public static TaskIndexer bootstrap(ElasticsearchParams elasticsearchParams, ElasticsearchClient es) {
		es.bootstrapElasticsearch(elasticsearchParams.getNumberOfShards(), elasticsearchParams.getNumberOfReplicas(), elasticsearchParams.getMaxTotalFields());
		mergingCronExp = elasticsearchParams.getMergingCronExp();

		String deletionCronExp = elasticsearchParams.getDeletionCronExp();
		if (!Strings.isEmpty(deletionCronExp)) {
			runDeletionTaskCron(deletionCronExp, es);
		}
		if (es.isWithPersistence()) {
			String persistentFetchCronExp = elasticsearchParams.getPersistentFetchCronExp();
			if (!Strings.isEmpty(persistentFetchCronExp)) {
				runPersistentFetchCron(persistentFetchCronExp, es);
			}
		}
		return new TaskIndexer(elasticsearchParams, es);
	}

	private static void runPersistentFetchCron(String persistentFetchCronExp, ElasticsearchClient es) {
		try {
			final StdSchedulerFactory sf = new StdSchedulerFactory();
			Scheduler scheduler = sf.getScheduler();
			JobDataMap jobDataMap = new JobDataMap();
			jobDataMap.put(ELASTIC_SEARCH_CLIENT, es);
			JobDetail job = newJob(PersistentFetchJob.class)
					.withIdentity("job2", "group2").usingJobData(jobDataMap)
					.build();
			CronTrigger trigger = newTrigger()
					.withIdentity("trigger2", "group2")
					.withSchedule(cronSchedule(persistentFetchCronExp))
					.build();

			scheduler.scheduleJob(job, trigger);
			scheduler.start();
		} catch (SchedulerException e) {
			LOG.error("Error occurred while fetching failed bulks from disk", e);
		}
	}

	public static DiskHandler getDiskHandler(String diskHandlerStrategy, Map<String, Object> params)  {
		String strategy = diskHandlerStrategy.toLowerCase();
		if (strategy.equals(SQLITE)){
			return new SQLJetDiskHandler(
					(int)params.get(SQLJetDiskHandler.MAX_FETCHED_BULKS_IN_ONE_TIME),
					(int)params.get(SQLJetDiskHandler.MAX_INSERT_TRIES),
					(String) params.get(SQLJetDiskHandler.LOCATION_IN_DISK)
			);
		}
		else{
			throw new RuntimeException("Unsupported disk handler strategy " + diskHandlerStrategy);
		}
	}

	public static void drainAndIndex(BlockingQueue<Event> eventsQueue, TaskIndexer taskIndexer, ElasticsearchClient es) {
		while (!eventsQueue.isEmpty() || es.hasFailedRequests()) {
			try {
				es.retryFailedRequestsFromMemory();

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
				//For refresh
				try {
					Thread.sleep(THREAD_SLEEP);
				} catch (InterruptedException e) {
					LOG.error("Error was thrown from TaskIndexer:", e);
				}
			} catch (RuntimeException e) {
				LOG.error("Error was thrown from TaskIndexer:", e);
			}
		}

	}

	public static long getTimesDuration(ZonedDateTime taskIndexerStartTime, ZonedDateTime taskIndexerEndTime) {
		return ChronoUnit.MILLIS.between(taskIndexerStartTime, taskIndexerEndTime);
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

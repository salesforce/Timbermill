package com.datorama.oss.timbermill.cron;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.oss.timbermill.ElasticsearchClient;
import com.datorama.oss.timbermill.TaskIndexer;
import com.datorama.oss.timbermill.common.ElasticsearchUtil;
import com.datorama.oss.timbermill.unit.AdoptedEvent;
import com.datorama.oss.timbermill.unit.Event;
import com.datorama.oss.timbermill.unit.Task;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import kamon.Kamon;
import kamon.metric.Metric;
import kamon.metric.Timer;

@DisallowConcurrentExecution
public class OrphansAdoptionJob implements Job {
	private static final Logger LOG = LoggerFactory.getLogger(OrphansAdoptionJob.class);

	private final Metric.Counter orphansFetchedCounter= Kamon.counter("timbermill2.orphans.fetched.counter");
	private final Metric.Counter orphansAdoptedCounter= Kamon.counter("timbermill2.orphans.adopted.counter");
	private final Metric.Timer orphansJobLatency = Kamon.timer("timbermill2.orphans.job.latency.timer");

	private final Random rand = new Random();

	@Override public void execute(JobExecutionContext context) {
		int secondsToWait = rand.nextInt(10);
		try {
			Thread.sleep(secondsToWait * 1000);
		} catch (InterruptedException ignored) {}

		Timer.Started started = orphansJobLatency.withoutTags().start();
		LOG.info("OrphansAdoptionJob started...");
		ElasticsearchClient es = (ElasticsearchClient) context.getJobDetail().getJobDataMap().get(ElasticsearchUtil.CLIENT);
		int partialOrphansGraceMinutes = context.getJobDetail().getJobDataMap().getInt(ElasticsearchUtil.PARTIAL_ORPHANS_GRACE_PERIOD_MINUTES);
		int orphansFetchPeriodMinutes = context.getJobDetail().getJobDataMap().getInt(ElasticsearchUtil.ORPHANS_FETCH_PERIOD_MINUTES);
		int daysRotationParam = context.getJobDetail().getJobDataMap().getInt(ElasticsearchUtil.DAYS_ROTATION);
		handleAdoptions(es, partialOrphansGraceMinutes, orphansFetchPeriodMinutes, daysRotationParam);
		LOG.info("OrphansAdoptionJob ended...");
		started.stop();
	}

	private void handleAdoptions(ElasticsearchClient es, int partialOrphansGraceMinutes, int orphansFetchMinutes, int daysRotation) {
		Map<String, Task> retrievedOrphans = es.getLatestOrphanIndexed(partialOrphansGraceMinutes, orphansFetchMinutes);
		LOG.info("Retrieved {} orphans", retrievedOrphans.size());
		orphansFetchedCounter.withoutTags().increment(retrievedOrphans.size());
		Set<String> orphansIds = retrievedOrphans.keySet();
		Set<String> parentsIds = retrievedOrphans.values().stream().map(Task::getParentId).collect(Collectors.toSet());
		Map<String, Task> fetchedParents = es.getMissingParents(orphansIds, parentsIds);
		Map<String, List<AdoptedEvent>> orphansByParentId = retrievedOrphans.entrySet().stream().map(entry -> new AdoptedEvent(entry.getKey(), entry.getValue())).
				collect(Collectors.groupingBy(AdoptedEvent::getParentId));

		Map<String, List<Event>> adoptedOrphans = adoptOrphanEvents(orphansByParentId, fetchedParents);
		Map<String, Task> adoptedTasksMap = TaskIndexer.getTasksFromEvents(adoptedOrphans, daysRotation);

		Map<String, Map<String, Task>> tasksPerEnv = Maps.newHashMap();
		adoptedTasksMap.forEach((taskId, task) -> {
			String env = task.getEnv();
			tasksPerEnv.computeIfAbsent(env, s -> Maps.newHashMap());
			tasksPerEnv.get(env).put(taskId, task);
		});

		tasksPerEnv.forEach((env,tasks) -> {
			String index = es.createTimbermillAlias(env);
			es.index(tasks, index);
			orphansAdoptedCounter.withoutTags().increment(tasks.size());
		});

		LOG.info("Adopted {} orphans", retrievedOrphans.size());
	}

	private Map<String, List<Event>> adoptOrphanEvents(Map<String, List<AdoptedEvent>> orphansByParent, Map<String, Task> fetchedParents) {
		Map<String, List<Event>> eventsMap = Maps.newHashMap();
		for (String taskId : fetchedParents.keySet()) {
			updateAdoptedOrphans(orphansByParent, eventsMap, fetchedParents, taskId);
		}
		return eventsMap;
	}

	private static void updateAdoptedOrphans(Map<String, List<AdoptedEvent>> orphansByParent, Map<String, List<Event>> adoptedEventsByParent, Map<String, Task> fetchedParents, String parentTaskId) {
		List<AdoptedEvent> adoptedEvents =  orphansByParent.get(parentTaskId);
		if (adoptedEvents != null) {
			for (AdoptedEvent adoptedEvent : adoptedEvents) {
				TaskIndexer.populateParentParams(adoptedEvent, fetchedParents.get(parentTaskId), adoptedEventsByParent.get(parentTaskId));
				String adoptedId = adoptedEvent.getTaskId();
				adoptedEventsByParent.computeIfAbsent(adoptedId, ignoreValue -> Lists.newArrayList());
				adoptedEventsByParent.get(adoptedId).add(adoptedEvent);
				updateAdoptedOrphans(orphansByParent, adoptedEventsByParent, fetchedParents, adoptedId);
			}
		}
	}
}

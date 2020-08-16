package com.datorama.oss.timbermill.cron;

import java.util.*;
import java.util.stream.Collectors;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.oss.timbermill.ElasticsearchClient;
import com.datorama.oss.timbermill.TaskIndexer;
import com.datorama.oss.timbermill.common.ElasticsearchUtil;
import com.datorama.oss.timbermill.common.KamonConstants;
import com.datorama.oss.timbermill.unit.AdoptedEvent;
import com.datorama.oss.timbermill.unit.Event;
import com.datorama.oss.timbermill.unit.Task;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import kamon.metric.Timer;

@DisallowConcurrentExecution
public class OrphansAdoptionJob implements Job {
	private static final Logger LOG = LoggerFactory.getLogger(OrphansAdoptionJob.class);

	private final Random rand = new Random();

	@Override public void execute(JobExecutionContext context) {
		int secondsToWait = rand.nextInt(10);
		try {
			Thread.sleep(secondsToWait * 1000);
		} catch (InterruptedException ignored) {}

		Timer.Started started = KamonConstants.ORPHANS_JOB_LATENCY.withoutTags().start();
		String flowId = "Orphans Adoption Job - " + UUID.randomUUID().toString();
		LOG.info("Flow ID: [{}]. Orphans Adoption Job started.", flowId);
		ElasticsearchClient es = (ElasticsearchClient) context.getJobDetail().getJobDataMap().get(ElasticsearchUtil.CLIENT);
		int partialOrphansGraceMinutes = context.getJobDetail().getJobDataMap().getInt(ElasticsearchUtil.PARTIAL_ORPHANS_GRACE_PERIOD_MINUTES);
		int orphansFetchPeriodMinutes = context.getJobDetail().getJobDataMap().getInt(ElasticsearchUtil.ORPHANS_FETCH_PERIOD_MINUTES);
		int daysRotationParam = context.getJobDetail().getJobDataMap().getInt(ElasticsearchUtil.DAYS_ROTATION);
		handleAdoptions(es, partialOrphansGraceMinutes, orphansFetchPeriodMinutes, daysRotationParam, flowId);
		LOG.info("Flow ID: [{}]. Orphans Adoption Job ended.", flowId);
		started.stop();
	}

	private void handleAdoptions(ElasticsearchClient es, int partialOrphansGraceMinutes, int orphansFetchMinutes, int daysRotation, String flowId) {
		Map<String, Task> latestOrphan = es.getLatestOrphanIndexed(partialOrphansGraceMinutes, orphansFetchMinutes, flowId);
		Map<String, Task> fetchedParents = fetchAdoptingParents(es, latestOrphan, flowId);
		Map<String, Task> adoptedTasksMap = enrichAdoptedOrphans(latestOrphan, fetchedParents, daysRotation);

		Map<String, Map<String, Task>> tasksPerIndex = Maps.newHashMap();

		adoptedTasksMap.forEach((taskId, task) -> {
			String index = task.getIndex();
			tasksPerIndex.computeIfAbsent(index, s -> Maps.newHashMap());
			tasksPerIndex.get(index).put(taskId, task);
		});

		tasksPerIndex.forEach((index,tasks) -> es.index(tasks, index, flowId));

		KamonConstants.ORPHANS_FOUND_HISTOGRAM.withoutTags().record(latestOrphan.size());
		KamonConstants.ORPHANS_ADOPTED_HISTOGRAM.withoutTags().record(adoptedTasksMap.size());
		if (!latestOrphan.isEmpty()) {
			LOG.info("Flow ID: [{}]. Found {} orphans, Adopted {} orphans.", flowId, latestOrphan.size(), adoptedTasksMap.size());
		}
		else {
			LOG.info("Flow ID: [{}]. Didn't find any orphans.", flowId);
		}
	}

	private Map<String, Task> enrichAdoptedOrphans(Map<String, Task> latestOrphan, Map<String, Task> fetchedParents, int daysRotation) {
		Map<String, List<AdoptedEvent>> orphansByParentId = latestOrphan.entrySet().stream().map(entry -> new AdoptedEvent(entry.getKey(), entry.getValue())).
				collect(Collectors.groupingBy(AdoptedEvent::getParentId));

		Map<String, List<Event>> adoptedOrphans = adoptOrphanEvents(orphansByParentId, fetchedParents);
		return TaskIndexer.getTasksFromEvents(adoptedOrphans, daysRotation);
	}

	private Map<String, Task> fetchAdoptingParents(ElasticsearchClient es, Map<String, Task> latestOrphan, String flowId) {
		Set<String> orphansIds = latestOrphan.keySet();
		Set<String> parentsIds = latestOrphan.values().stream().map(Task::getParentId).collect(Collectors.toSet());
		return es.getMissingParents(orphansIds, parentsIds, flowId);
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

package com.datorama.oss.timbermill.cron;

import com.datorama.oss.timbermill.common.KamonConstants;
import com.datorama.oss.timbermill.common.persistence.PersistenceHandler;
import com.datorama.oss.timbermill.pipe.LocalOutputPipe;
import com.datorama.oss.timbermill.unit.Event;
import kamon.metric.Timer;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;

import static com.datorama.oss.timbermill.common.ElasticsearchUtil.*;

@DisallowConcurrentExecution
public class EventsPersistentFetchJob implements Job {

	private static final Logger LOG = LoggerFactory.getLogger(EventsPersistentFetchJob.class);

	@Override public void execute(JobExecutionContext context) {
		PersistenceHandler persistenceHandler = (PersistenceHandler) context.getJobDetail().getJobDataMap().get(PERSISTENCE_HANDLER);
		BlockingQueue<Event> eventsQueue = (BlockingQueue<Event>) context.getJobDetail().getJobDataMap().get(EVENTS_QUEUE);
		BlockingQueue<Event> overflowedQueue = (BlockingQueue<Event>) context.getJobDetail().getJobDataMap().get(OVERFLOWED_EVENTS_QUEUE);
		if (persistenceHandler != null && hasEnoughRoomLeft(eventsQueue)) {
			KamonConstants.CURRENT_DATA_IN_DB_GAUGE.withTag("type", "overflowed_events_lists_amount").update(persistenceHandler.overFlowedEventsListsAmount());
			String flowId = "Overflowed Event Persistent Fetch Job - " + UUID.randomUUID().toString();
			MDC.put("id", flowId);
			LOG.info("Overflowed Events Fetch Job started.");
			Timer.Started start = KamonConstants.EVENTS_FETCH_JOB_LATENCY.withoutTags().start();
			while (hasEnoughRoomLeft(eventsQueue)){
				List<Event> events = persistenceHandler.fetchAndDeleteOverflowedEvents();
				if (events.isEmpty()){
					break;
				}
				else {
					for (Event event : events) {
						LocalOutputPipe.pushEventToQueues(persistenceHandler, eventsQueue, overflowedQueue, event);
					}
				}
			}
			start.stop();
			LOG.info("Overflowed Events Fetch Job ended.");
		}
	}

	private boolean hasEnoughRoomLeft(BlockingQueue<Event> eventsQueue) {
		double threshold = (eventsQueue.remainingCapacity() + eventsQueue.size()) * 0.2; // Only when queue had at least 20% free we will start adding persistent events
		return eventsQueue.remainingCapacity() > threshold;
	}

}

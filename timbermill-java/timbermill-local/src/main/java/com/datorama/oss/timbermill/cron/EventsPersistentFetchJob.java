package com.datorama.oss.timbermill.cron;

import com.datorama.oss.timbermill.common.KamonConstants;
import com.datorama.oss.timbermill.common.persistence.PersistenceHandler;
import com.datorama.oss.timbermill.pipe.LocalOutputPipe;
import com.datorama.oss.timbermill.unit.Event;
import com.google.common.cache.LoadingCache;
import io.github.resilience4j.ratelimiter.RateLimiter;
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
	private boolean first = true;

	@Override public void execute(JobExecutionContext context) {
		PersistenceHandler persistenceHandler = (PersistenceHandler) context.getJobDetail().getJobDataMap().get(PERSISTENCE_HANDLER);
		BlockingQueue<Event> eventsQueue = (BlockingQueue<Event>) context.getJobDetail().getJobDataMap().get(EVENTS_QUEUE);
		BlockingQueue<Event> overflowedQueue = (BlockingQueue<Event>) context.getJobDetail().getJobDataMap().get(OVERFLOWED_EVENTS_QUEUE);
		LoadingCache<String, RateLimiter> rateLimiterMap = (LoadingCache<String, RateLimiter>) context.getJobDetail().getJobDataMap().get(RATE_LIMITER_MAP);
		if (persistenceHandler != null && hasEnoughRoomLeft(eventsQueue)) {
			KamonConstants.CURRENT_DATA_IN_DB_GAUGE.withTag("type", "overflowed_events_lists_amount").update(persistenceHandler.overFlowedEventsListsAmount());
			String flowId = "Overflowed Event Persistent Fetch Job - " + UUID.randomUUID().toString();
			MDC.put("id", flowId);
			LOG.info("Overflowed Events Fetch Job started.");
			Timer.Started start = KamonConstants.EVENTS_FETCH_JOB_LATENCY.withoutTags().start();
			int amountOfEventsFetched = 0;
			int amountOfEventIngested = 0;
			while (hasEnoughRoomLeft(eventsQueue)){
				List<Event> events = persistenceHandler.fetchAndDeleteOverflowedEvents();
				if (events == null){
					break;
				}
				else {
					amountOfEventsFetched += events.size();
					for (Event event : events) {
						try {
							if (event != null) {
								LocalOutputPipe.doPushEventToQueues(persistenceHandler, eventsQueue, overflowedQueue, event)
								amountOfEventIngested++;
							}
						} catch (Exception e){
							if (first) {
								LOG.error("First ERROR, An error occured when trying to write event to queue", e);
								first = false;
							}
						}
					}

				}
			}
			start.stop();
			LOG.info("Fetched {} overflowed events, ingested {}", amountOfEventsFetched, amountOfEventIngested);
			LOG.info("Overflowed Events Fetch Job ended.");
		}
	}

	private boolean hasEnoughRoomLeft(BlockingQueue<Event> eventsQueue) {
		double threshold = (eventsQueue.remainingCapacity() + eventsQueue.size()) * 0.2; // Only when queue had at least 20% free we will start adding persistent events
		return eventsQueue.remainingCapacity() > threshold;
	}

}

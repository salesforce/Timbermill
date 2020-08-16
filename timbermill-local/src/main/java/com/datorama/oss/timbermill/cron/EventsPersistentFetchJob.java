package com.datorama.oss.timbermill.cron;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.oss.timbermill.common.KamonConstants;
import com.datorama.oss.timbermill.common.disk.DiskHandler;
import com.datorama.oss.timbermill.unit.Event;

import kamon.metric.Timer;
import static com.datorama.oss.timbermill.common.ElasticsearchUtil.*;

@DisallowConcurrentExecution
public class EventsPersistentFetchJob implements Job {

	private static final Logger LOG = LoggerFactory.getLogger(EventsPersistentFetchJob.class);

	@Override public void execute(JobExecutionContext context) {
		DiskHandler diskHandler = (DiskHandler) context.getJobDetail().getJobDataMap().get(DISK_HANDLER);
		BlockingQueue<Event> eventsQueue = (BlockingQueue<Event>) context.getJobDetail().getJobDataMap().get(EVENTS_QUEUE);
		BlockingQueue<Event> overflowedQueue = (BlockingQueue<Event>) context.getJobDetail().getJobDataMap().get(OVERFLOWED_EVENTS_QUEUE);
		if (diskHandler != null && hasEnoughRoomLeft(eventsQueue)) {
			String flowId = "Overflowed Event Persistent Fetch Job - " + UUID.randomUUID().toString();
			LOG.info("Flow ID: [{}] Overflowed Events Fetch Job started.", flowId);
			Timer.Started start = KamonConstants.EVENTS_FETCH_JOB_LATENCY.withoutTags().start();
			while (hasEnoughRoomLeft(eventsQueue)){
				List<Event> events = diskHandler.fetchAndDeleteOverflowedEvents(flowId);
				if (events.isEmpty()){
					break;
				}
				else {
					for (Event event : events) {
						if(!eventsQueue.offer(event)){
							if (!overflowedQueue.offer(event)){
								LOG.error("Flow ID: [{}] OverflowedQueue is full, event {} was discarded", flowId, event.getTaskId());
							}
							else {
								KamonConstants.MESSAGES_IN_OVERFLOWED_QUEUE_RANGE_SAMPLER.withoutTags().increment();
							}
						}
						else{
							KamonConstants.MESSAGES_IN_INPUT_QUEUE_RANGE_SAMPLER.withoutTags().increment();
						}
					}
				}
			}
			start.stop();
			LOG.info("Flow ID: [{}] Overflowed Events Fetch Job ended.", flowId);
		}
	}

	private boolean hasEnoughRoomLeft(BlockingQueue<Event> eventsQueue) {
		double threshold = (eventsQueue.remainingCapacity() + eventsQueue.size()) * 0.2; // Only when queue had at least 20% free we will start adding persistent events
		return eventsQueue.remainingCapacity() > threshold;
	}

}

package com.datorama.oss.timbermill.cron;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.oss.timbermill.common.disk.DiskHandler;
import com.datorama.oss.timbermill.unit.Event;

import static com.datorama.oss.timbermill.common.ElasticsearchUtil.*;

public class EventsPersistentFetchJob implements Job {

	private static final Logger LOG = LoggerFactory.getLogger(EventsPersistentFetchJob.class);
	private static boolean currentlyRunning = false;

	@Override public void execute(JobExecutionContext context) {
		if (!currentlyRunning) {
			currentlyRunning = true;
			LOG.info("Cron is fetching overflowed events from disk...");
			DiskHandler diskHandler = (DiskHandler) context.getJobDetail().getJobDataMap().get(DISK_HANDLER);
			BlockingQueue<Event> eventsQueue = (BlockingQueue<Event>) context.getJobDetail().getJobDataMap().get(EVENTS_QUEUE);
			BlockingQueue<Event> overflowedQueue = (BlockingQueue<Event>) context.getJobDetail().getJobDataMap().get(OVERFLOWED_EVENTS_QUEUE);
			while (hasEnoughRoomLeft(eventsQueue)){
				List<Event> events = diskHandler.fetchAndDeleteOverflowedEvents();
				if (events.isEmpty()){
					break;
				}
				else {
					for (Event event : events) {
						if (!eventsQueue.offer(event)) {
							if (!overflowedQueue.offer(event)) {
								LOG.error("OverflowedQueue is full, event {} was discarded", event.getTaskId());
							}
						}
					}
				}
			}
			currentlyRunning = false;
		}
	}

	private boolean hasEnoughRoomLeft(BlockingQueue<Event> eventsQueue) {
		double threshold = (eventsQueue.remainingCapacity() + eventsQueue.size()) * 0.2; // Only when queue had at least 20% free we will start adding persistent events
		return eventsQueue.remainingCapacity() > threshold;
	}

}

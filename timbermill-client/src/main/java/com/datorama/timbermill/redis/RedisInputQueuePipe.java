package com.datorama.timbermill.redis;

import com.datorama.timbermill.DateTimeTypeConverter;
import com.datorama.timbermill.Event;
import com.datorama.timbermill.EventInputPipe;
import com.datorama.timbermill.redis.datastructures.JedisQueueImpl;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class RedisInputQueuePipe implements EventInputPipe{

	private JedisQueueImpl<String> redisBlockingQueue;

	public RedisInputQueuePipe(String queueName, RedisTemplate redisTemplate) {
		redisBlockingQueue = new JedisQueueImpl<String>(queueName, redisTemplate);
	}

	private static final Logger LOG = LoggerFactory.getLogger(RedisInputQueuePipe.class);

	@Override
	public List<Event> read(int maxEvent) {
		Collection<String> eventsStrings = new ArrayList<>();
		redisBlockingQueue.drainTo(eventsStrings, maxEvent);
		Gson gson = new GsonBuilder().registerTypeAdapter(DateTime.class, new DateTimeTypeConverter()).create();
		List<Event> events = new ArrayList<>();
		for (String eventString : eventsStrings) {
			try {
				events.add(gson.fromJson(eventString, Event.class));
			} catch (RuntimeException e){
				LOG.error("Could nor parse string {}", eventString);
				LOG.error("Error thrown: {} \n {}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			}
		}
		return events;
	}

}

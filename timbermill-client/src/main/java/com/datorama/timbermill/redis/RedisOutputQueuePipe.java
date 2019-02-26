package com.datorama.timbermill.redis;

import com.datorama.timbermill.DateTimeTypeConverter;
import com.datorama.timbermill.Event;
import com.datorama.timbermill.EventOutputPipe;
import com.datorama.timbermill.redis.datastructures.JedisQueueImpl;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Queue;

public class RedisOutputQueuePipe implements EventOutputPipe{

	private final String queueName;
	private final int maxQueueSize;
	private Queue<String> redisBlockingQueue;
	private final RedisTemplate redisTemplate;
	private static final Logger log = LoggerFactory.getLogger(RedisOutputQueuePipe.class);
	private Gson gson = new GsonBuilder().registerTypeAdapter(DateTime.class, new DateTimeTypeConverter()).create();

	public RedisOutputQueuePipe(String queueName, int maxQueueSize, RedisTemplate redisTemplate) {
		this.queueName = queueName;
		this.maxQueueSize = maxQueueSize;
		try {
			this.redisTemplate = redisTemplate;
			log.info("Timbermill Redis Client (jedis) created.");
		} catch (Exception e) {
			log.error("Timbermill Redis is unavailable");
			throw new RuntimeException(e);
		}
		connectToRedisQueue();
	}

	private void connectToRedisQueue() {
		try {
			redisBlockingQueue = new JedisQueueImpl<String>(queueName, redisTemplate);
			log.info("Timbermill Redis Queue created.");
		}
		catch (Exception ignored){
			log.error("Timbermill Redis Queue unavailable");
		}
	}

	@Override
	public void send(Event e) {
		if (redisBlockingQueue == null){
			connectToRedisQueue();
		}
		if (redisBlockingQueue.size() >= maxQueueSize){
			String eventString = redisBlockingQueue.poll();
			Event event = gson.fromJson(eventString, Event.class);
			log.warn("Event {} was removed from the queue due to insufficient space", event.getTaskId());
		}
		String eventString;
		try {
			eventString = gson.toJson(e);
		}
		catch (Exception exc){
			log.error("Timbermill Client Error: GSON was unable to parse event " + e.getTaskId(), exc );
			return;
		}
		redisBlockingQueue.add(eventString);
	}

	@Override
	public int getMaxQueueSize() {
		return maxQueueSize;
	}

}

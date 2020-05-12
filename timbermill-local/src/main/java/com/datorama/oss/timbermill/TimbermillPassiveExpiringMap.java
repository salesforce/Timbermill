package com.datorama.oss.timbermill;

import java.util.Queue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections4.map.PassiveExpiringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.oss.timbermill.unit.AdoptedEvent;

public class TimbermillPassiveExpiringMap extends PassiveExpiringMap<String, Queue<AdoptedEvent>> {
	private static final Logger LOG = LoggerFactory.getLogger(TimbermillPassiveExpiringMap.class);

	private final int maximumCacheSize;

	TimbermillPassiveExpiringMap(int maximumOrphansMinutesHold, TimeUnit timeUnit, int maximumCacheSize) {
		super(maximumOrphansMinutesHold, timeUnit);
		this.maximumCacheSize = maximumCacheSize;
	}

	@Override
	public Queue<AdoptedEvent> put(String key, Queue<AdoptedEvent> value) {
		if (size() + 1 > maximumCacheSize){
			LOG.warn("Orphan {} was not saved to orphan map, map is full.", key);
			return null;
		}
		else{
			return super.put(key, value);
		}
	}

}


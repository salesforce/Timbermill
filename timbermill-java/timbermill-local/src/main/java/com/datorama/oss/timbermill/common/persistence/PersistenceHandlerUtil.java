package com.datorama.oss.timbermill.common.persistence;

import com.datorama.oss.timbermill.common.redis.RedisService;

import java.util.Map;

public class PersistenceHandlerUtil {

	public static PersistenceHandler getPersistenceHandler(String persistenceHandlerStrategy,
														   Map<String, Object> params) {
		PersistenceHandler persistenceHandler = null;
		if (persistenceHandlerStrategy != null && !persistenceHandlerStrategy.toLowerCase().equals("none")){
			persistenceHandler = getPersistenceHandlerByStrategy(persistenceHandlerStrategy, params);
			if (!persistenceHandler.isCreatedSuccessfully()){
				persistenceHandler = null;
			}
		}
		return persistenceHandler;
	}

	private static PersistenceHandler getPersistenceHandlerByStrategy(String persistenceHandlerStrategy, Map<String, Object> params) {
		String strategy = persistenceHandlerStrategy.toLowerCase();
		switch (strategy) {
			case "sqlite":
				return new SQLJetPersistenceHandler(
						(int) params.get(PersistenceHandler.MAX_FETCHED_BULKS_IN_ONE_TIME),
						(int) params.get(PersistenceHandler.MAX_FETCHED_EVENTS_IN_ONE_TIME),
						(int) params.get(PersistenceHandler.MAX_INSERT_TRIES),
						(String) params.get(SQLJetPersistenceHandler.LOCATION_IN_DISK));
			case "redis":
				return new RedisPersistenceHandler(
						(int) params.get(PersistenceHandler.MAX_FETCHED_BULKS_IN_ONE_TIME),
						(int) params.get(PersistenceHandler.MAX_FETCHED_EVENTS_IN_ONE_TIME),
						(int) params.get(PersistenceHandler.MAX_INSERT_TRIES),
						(long) params.get(RedisPersistenceHandler.MIN_LIFETIME),
						(int) params.get(RedisPersistenceHandler.TTL),
						(RedisService) params.get(RedisPersistenceHandler.REDIS_SERVICE));
			default:
				throw new RuntimeException("Unsupported persistence handler strategy " + persistenceHandlerStrategy);
		}

	}
}

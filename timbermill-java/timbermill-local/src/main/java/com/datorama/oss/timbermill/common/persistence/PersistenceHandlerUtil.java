package com.datorama.oss.timbermill.common.persistence;

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

	private static PersistenceHandler getPersistenceHandlerByStrategy(String persistenceHandlerStrategy, Map<String, Object> params)  {
		String strategy = persistenceHandlerStrategy.toLowerCase();
		if (strategy.equals("sqlite")){
			return new SQLJetPersistenceHandler(
					(int)params.get(SQLJetPersistenceHandler.MAX_FETCHED_BULKS_IN_ONE_TIME),
					(int)params.get(SQLJetPersistenceHandler.MAX_INSERT_TRIES),
					(String) params.get(SQLJetPersistenceHandler.LOCATION_IN_DISK)
			);
		}
		else{
			throw new RuntimeException("Unsupported persistence handler strategy " + persistenceHandlerStrategy);
		}
	}
}

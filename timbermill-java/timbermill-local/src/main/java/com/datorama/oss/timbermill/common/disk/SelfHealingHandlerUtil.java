package com.datorama.oss.timbermill.common.disk;

import java.util.Map;

public class SelfHealingHandlerUtil {

	public static selfHealingHandler getSelfHealingHandler(String diskHandlerStrategy,
														   Map<String, Object> params) {
		selfHealingHandler selfHealingHandler = null;
		if (diskHandlerStrategy != null && !diskHandlerStrategy.toLowerCase().equals("none")){
			selfHealingHandler = getSelfHealingHandlerByStrategy(diskHandlerStrategy, params);
			if (!selfHealingHandler.isCreatedSuccessfully()){
				selfHealingHandler = null;
			}
		}
		return selfHealingHandler;
	}

	private static selfHealingHandler getSelfHealingHandlerByStrategy(String diskHandlerStrategy, Map<String, Object> params)  {
		String strategy = diskHandlerStrategy.toLowerCase();
		if (strategy.equals("sqlite")){
			return new SQLJetHandler(
					(int)params.get(SQLJetHandler.MAX_FETCHED_BULKS_IN_ONE_TIME),
					(int)params.get(SQLJetHandler.MAX_INSERT_TRIES),
					(String) params.get(SQLJetHandler.LOCATION_IN_DISK)
			);
		}
		else{
			throw new RuntimeException("Unsupported disk handler strategy " + diskHandlerStrategy);
		}
	}
}

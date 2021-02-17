package com.datorama.oss.timbermill.common.disk;

import java.util.Map;

public class DiskHandlerUtil {

	public static DiskHandler getDiskHandler(String diskHandlerStrategy,
			Map<String, Object> params) {
		DiskHandler diskHandler = null;
		if (diskHandlerStrategy != null && !diskHandlerStrategy.toLowerCase().equals("none")){
			diskHandler = getDiskHandlerByStrategy(diskHandlerStrategy, params);
			if (!diskHandler.isCreatedSuccessfully()){
				diskHandler = null;
			}
		}
		return diskHandler;
	}

	private static DiskHandler getDiskHandlerByStrategy(String diskHandlerStrategy, Map<String, Object> params)  {
		String strategy = diskHandlerStrategy.toLowerCase();
		if (strategy.equals("sqlite")){
			return new SQLJetDiskHandler(
					(int)params.get(SQLJetDiskHandler.MAX_FETCHED_BULKS_IN_ONE_TIME),
					(int)params.get(SQLJetDiskHandler.MAX_INSERT_TRIES),
					(String) params.get(SQLJetDiskHandler.LOCATION_IN_DISK)
			);
		}
		else{
			throw new RuntimeException("Unsupported disk handler strategy " + diskHandlerStrategy);
		}
	}
}

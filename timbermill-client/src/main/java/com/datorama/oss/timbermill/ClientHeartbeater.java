package com.datorama.oss.timbermill;

import com.datorama.oss.timbermill.common.Constants;
import com.datorama.oss.timbermill.pipe.EventOutputPipe;
import com.datorama.oss.timbermill.pipe.StatisticsCollectorOutputPipe;
import com.datorama.oss.timbermill.unit.LogParams;
import com.datorama.oss.timbermill.unit.TaskStatus;

public class ClientHeartbeater {

	private static final int TIME_TO_SLEEP = 60000;

	public static final String SUBMIT_AMOUNT = "submitAmount";
	public static final String AVG_SUBMIT_DURATION = "avgSubmitDuration";
	public static final String MAX_SUBMIT_DURATION = "maxSubmitDuration";
	public static final String OUTPUT_BUFFER_SIZE = "outputBufferSize";

	private final StatisticsCollectorOutputPipe statsCollector;

	private final EventOutputPipe eop;

	ClientHeartbeater(StatisticsCollectorOutputPipe statsCollector, EventOutputPipe eop) {
		this.statsCollector = statsCollector;
		this.eop = eop;
	}

	void start() {
		Thread heartbeatThread = new Thread(() -> {
			while (true) {
				LogParams logParams = LogParams.create().metric(SUBMIT_AMOUNT, statsCollector.getEventsAmount())
						.metric(AVG_SUBMIT_DURATION, statsCollector.getAvgSubmitDuration()).metric(MAX_SUBMIT_DURATION, statsCollector.getMaxSubmitDuration());
				statsCollector.initCounters();
				if (eop != null) {
					logParams.metric(OUTPUT_BUFFER_SIZE, eop.getCurrentBufferSize());
				}
				EventLogger.get().spotEvent(null, Constants.HEARTBEAT_TASK, null, logParams, TaskStatus.SUCCESS, null);
				try {
					Thread.sleep(TIME_TO_SLEEP);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		heartbeatThread.setName("com.datorama.com.datorama.oss.timbermill-heartbeat-thread");
		heartbeatThread.setDaemon(true);
		heartbeatThread.start();
	}

}

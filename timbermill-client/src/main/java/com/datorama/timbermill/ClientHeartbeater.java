package com.datorama.timbermill;

import com.datorama.timbermill.common.Constants;
import com.datorama.timbermill.unit.LogParams;
import com.datorama.timbermill.unit.Task;

public class ClientHeartbeater {

	private static final int TIME_TO_SLEEP = 60000;

	public static final String SUBMIT_AMOUNT = "submitAmount";
	public static final String AVG_SUBMIT_DURATION = "avgSubmitDuration";
	public static final String MAX_SUBMIT_DURATION = "maxSubmitDuration";
	public static final String OUTPUT_BUFFER_SIZE = "outputBufferSize";

	private final StatisticsCollectorOutputPipe statsCollector;

	private final BufferingOutputPipe bop;

	ClientHeartbeater(StatisticsCollectorOutputPipe statsCollector, BufferingOutputPipe bop) {
		this.statsCollector = statsCollector;
		this.bop = bop;
	}

	void start() {
		Thread heartbeatThread = new Thread(() -> {
			while (true) {
				LogParams logParams = LogParams.create().metric(SUBMIT_AMOUNT, statsCollector.getEventsAmount())
						.metric(AVG_SUBMIT_DURATION, statsCollector.getAvgSubmitDuration()).metric(MAX_SUBMIT_DURATION, statsCollector.getMaxSubmitDuration());
				statsCollector.initCounters();
				if (bop != null) {
					logParams.metric(OUTPUT_BUFFER_SIZE, bop.getCurrentBufferSize());
				}
				EventLogger.get().spotEvent(null, Constants.HEARTBEAT_TASK, null, logParams, Task.TaskStatus.SUCCESS, null);
				try {
					Thread.sleep(TIME_TO_SLEEP);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		heartbeatThread.setName("com.datorama.timbermill-heartbeat-thread");
		heartbeatThread.setDaemon(true);
		heartbeatThread.start();
	}

}

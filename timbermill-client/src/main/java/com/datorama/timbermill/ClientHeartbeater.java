package com.datorama.timbermill;

import com.datorama.timbermill.common.Constants;
import com.datorama.timbermill.pipe.BufferingOutputPipe;
import com.datorama.timbermill.pipe.StatisticsCollectorOutputPipe;
import com.datorama.timbermill.unit.LogParams;
import com.datorama.timbermill.unit.Task;

public class ClientHeartbeater {

	private static final int TIME_TO_SLEEP = 60000;

	public static final String SUBMIT_AMOUNT = "submitAmount";
	public static final String AVG_SUBMIT_DURATION = "avgSubmitDuration";
	public static final String MAX_SUBMIT_DURATION = "maxSubmitDuration";
	public static final String OUTPUT_BUFFER_SIZE = "outputBufferSize";

	private StatisticsCollectorOutputPipe statsCollector;

	private BufferingOutputPipe bop;

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
				EventLogger.get().spotEvent(Constants.HEARTBEAT_TASK, logParams, Task.TaskStatus.SUCCESS);
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

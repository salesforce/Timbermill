package com.datorama.timbermill;

import com.datorama.timbermill.common.Constants;
import com.datorama.timbermill.pipe.BufferingOutputPipe;
import com.datorama.timbermill.pipe.StatisticsCollectorOutputPipe;

import java.util.HashMap;
import java.util.Map;

public class ClientHeartbeater {

	private static final int MIN = 60000;

	private static final String SUBMIT_AMOUNT = "submitAmount";
	private static final String AVG_SUBMIT_DURATION = "avgSubmitDuration";
	private static final String MAX_SUBMIT_DURATION = "maxSubmitDuration";
	private static final String OUTPUT_BUFFER_SIZE = "outputBufferSize";

	private StatisticsCollectorOutputPipe statsCollector;

	private BufferingOutputPipe bop;

	ClientHeartbeater(StatisticsCollectorOutputPipe statsCollector, BufferingOutputPipe bop) {
		this.statsCollector = statsCollector;
		this.bop = bop;
	}

	public void start() {
		Thread heartbeatThread = new Thread(() -> {
			while (true) {
				Map<String, Number> metrics = new HashMap<>();
				metrics.put(SUBMIT_AMOUNT, statsCollector.getEventsAmount());
				metrics.put(AVG_SUBMIT_DURATION, statsCollector.getAvgSubmitDuration());
				metrics.put(MAX_SUBMIT_DURATION, statsCollector.getMaxSubmitDuration());

				statsCollector.initCounters();

				if (bop != null) {
					metrics.put(OUTPUT_BUFFER_SIZE, bop.getCurrentBufferSize());
				}

				EventLogger.get().spotEvent(Constants.HEARTBEAT_TASK, null, metrics, null, null);


				try {
					Thread.sleep(MIN);
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

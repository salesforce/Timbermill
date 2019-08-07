package com.datorama.timbermill.pipe;

import com.datorama.timbermill.TaskIndexer;
import com.datorama.timbermill.unit.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class LocalOutputPipe implements EventOutputPipe {

    private TaskIndexer taskIndexer;
    private Map<String, String> staticParams;

    private boolean keepRunning = true;
    private BlockingQueue<Event> eventsQueue = new ArrayBlockingQueue<>(1000000);
    private boolean stoppedRunning = false;

    private static final Logger LOG = LoggerFactory.getLogger(LocalOutputPipe.class);

    public LocalOutputPipe(LocalOutputPipeConfig config) {
        taskIndexer = new TaskIndexer(config);
        staticParams = config.getStaticParams();
        new Thread(() -> {
            taskIndexer.indexStartupEvent();

            try {
                while (keepRunning) {
                    long l1 = System.currentTimeMillis();
                    try {
                        List<Event> events = new ArrayList<>();
                        eventsQueue.drainTo(events, config.getIndexBulkSize());
                        taskIndexer.retrieveAndIndex(events);
                    } catch (RuntimeException e) {
                        LOG.error("Error was thrown from TaskIndexer:", e);
                    } finally {
                        long l2 = System.currentTimeMillis();
                        long timeToSleep = (config.getSecondBetweenPolling() * 1000) - (l2 - l1);
                        Thread.sleep(Math.max(timeToSleep, 0));
                    }
                }
                stoppedRunning = true;
            }catch (InterruptedException ignore){
                LOG.info("Timbermill server was interrupted, exiting");
            }
        }).start();
    }

    @Override
    public void send(Event e){
        eventsQueue.add(e);
    }

    public void close() {
        taskIndexer.close();
        keepRunning = false;
        while(!stoppedRunning){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
            }
        }
    }

    @Override
    public Map<String, String> getStaticParams() {
        return staticParams;
    }
}

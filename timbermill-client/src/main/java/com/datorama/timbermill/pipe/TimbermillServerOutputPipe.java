package com.datorama.timbermill.pipe;

import com.datorama.timbermill.unit.Event;
import com.datorama.timbermill.unit.EventsWrapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class TimbermillServerOutputPipe implements EventOutputPipe {

    private static final Logger LOG = LoggerFactory.getLogger(TimbermillServerOutputPipe.class);
    private URL timbermillServerUrl;
    private List<Event> buffer;
    private Thread thread;
    private int maxEventsBatchSize;
    private long maxSecondsBeforeBatchTimeout;


    private TimbermillServerOutputPipe() {
    }

    private TimbermillServerOutputPipe(Builder builder){
        if (builder.timbermillServerUrl == null){
            throw new RuntimeException("Must enclose the Timbermill server URL");
        }
        try {
            timbermillServerUrl = new URL(builder.timbermillServerUrl);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
        maxEventsBatchSize = builder.maxEventsBatchSize;
        maxSecondsBeforeBatchTimeout = builder.maxSecondsBeforeBatchTimeout;
        buffer = Collections.synchronizedList(new LinkedList<>());
    }

    private void startThread() {
        thread = new Thread(() -> {
            do {
                try {

                    List<Event> eventsToSend = createEventsToSend();
                    if (eventsToSend != null) {
                        EventsWrapper eventsWrapper = new EventsWrapper(eventsToSend);
                        HttpURLConnection httpCon = (HttpURLConnection) timbermillServerUrl.openConnection();
                        httpCon.setRequestMethod("POST");
                        httpCon.setRequestProperty("content-type", "application/json");
                        httpCon.setDoOutput(true);
                        ObjectMapper om = new ObjectMapper();
                        ObjectWriter ow = om.writerFor(om.getTypeFactory().constructType(EventsWrapper.class));
                        String eventsStr = ow.writeValueAsString(eventsWrapper);
                        OutputStream os = httpCon.getOutputStream();
                        os.write(eventsStr.getBytes());
                        os.flush();
                        os.close();
                        int responseCode = httpCon.getResponseCode();
                        if (responseCode != 200){
                            LOG.warn("Request to timbermill return status {}, Message: {}", responseCode, httpCon.getResponseMessage());
                        }
                        else{
                            LOG.debug("{} were sent to Timbermill server", eventsToSend.size());
                        }
                    }
                } catch (Exception e) {
                    LOG.error("Error sending events to Timbermill server" ,e);
                }
            } while (buffer.size() > 0);
        });
        thread.start();
    }

    private List<Event> createEventsToSend() throws JsonProcessingException {
        long startBatchTime = System.currentTimeMillis();
        List<Event> eventsToSend = new ArrayList<>();
        int currentBatchSize = 0;
        if(!buffer.isEmpty()) {
            Event event = buffer.remove(0);
            currentBatchSize += getEventSize(event);
            eventsToSend.add(event);

            while(!buffer.isEmpty() && currentBatchSize <= this.maxEventsBatchSize && !isExceededMaxTimeToWait(startBatchTime)) {
                int eventSize = getEventSize(buffer.get(0));
                if(currentBatchSize + eventSize <= this.maxEventsBatchSize) {
                    event = buffer.remove(0);
                    eventsToSend.add(event);
                    currentBatchSize += eventSize;
                }
                else {
                    break;
                }
            }
        }
        else {
            return null;
        }
        return eventsToSend;
    }

    private boolean isExceededMaxTimeToWait(long startBatchTime) {
        return System.currentTimeMillis() - startBatchTime > maxSecondsBeforeBatchTimeout * 1000;
    }

    private int getEventSize(Event event) throws JsonProcessingException {
        ObjectMapper om = new ObjectMapper();
        String eventStr = om.writeValueAsString(event);
        return eventStr.getBytes().length;
    }

    @Override
    public void send(Event e) {
        synchronized(this) {
            this.buffer.add(e);
            if (this.thread == null || !this.thread.isAlive()) {
                this.startThread();
            }
        }
    }

    @Override
    public void close() {
    }

    public static class Builder {
        private String timbermillServerUrl;
        private int maxEventsBatchSize = 2097152; // 2mb
        private long maxSecondsBeforeBatchTimeout = 3;


        public TimbermillServerOutputPipe.Builder timbermillServerUrl(String timbermillServerUrl) {
            this.timbermillServerUrl = timbermillServerUrl;
            return this;
        }

        public TimbermillServerOutputPipe.Builder maxEventsBatchSize(int maxEventsBatchSize) {
            this.maxEventsBatchSize = maxEventsBatchSize;
            return this;
        }

        public TimbermillServerOutputPipe.Builder maxSecondsBeforeBatchTimeout(long maxSecondsBeforeBatchTimeout) {
            this.maxSecondsBeforeBatchTimeout = maxSecondsBeforeBatchTimeout;
            return this;
        }

        public TimbermillServerOutputPipe build() {
            return new TimbermillServerOutputPipe(this);
        }

    }
}
package com.datorama.timbermill.pipe;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.timbermill.unit.Event;
import com.datorama.timbermill.unit.EventsWrapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

public class TimbermillServerOutputPipe implements EventOutputPipe {

    private static final int TWO_MB = 2097152;
    private static final int HTTP_TIMEOUT = 2000;
    private static final int MAX_RETRY = 3;
    private static final Logger LOG = LoggerFactory.getLogger(TimbermillServerOutputPipe.class);
    private static volatile boolean keepRunning = true;
    private URL timbermillServerUrl;
    private LinkedBlockingQueue<Event> buffer;
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
        buffer = new LinkedBlockingQueue<>();
        thread = new Thread(() -> {
            do {
                try {
                    List<Event> eventsToSend = getEventsToSend();
                    if (!eventsToSend.isEmpty()) {
                        EventsWrapper eventsWrapper = new EventsWrapper(eventsToSend);
                        sendEvents(eventsToSend, eventsWrapper);
                    }
                } catch (Exception e) {
                    LOG.error("Error sending events to Timbermill server" ,e);
                }
            } while (keepRunning);
        });
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            keepRunning = false;
            try {
                thread.join();
            } catch (InterruptedException ignored) {
            }
        }));
    }

    private void sendEvents(List<Event> eventsToSend, EventsWrapper eventsWrapper) throws IOException {
        HttpURLConnection httpCon = getHttpURLConnection();
        byte[] eventsWrapperBytes = getEventsWrapperBytes(eventsWrapper);
        for (int tryNum = 1; tryNum <= MAX_RETRY; tryNum++) {
            sendEventsOverConnection(httpCon, eventsWrapperBytes);
            int responseCode = httpCon.getResponseCode();
            if (responseCode == 200) {
                LOG.debug("{} were sent to Timbermill server", eventsToSend.size());
                return;

            } else {
                LOG.warn("Request to timbermill return status {}, Attempt: {}//{} Message: {}", responseCode, tryNum, MAX_RETRY, httpCon.getResponseMessage());
            }
        }
        LOG.error("Can't send events to timbermill, failed {} attempts." , MAX_RETRY);
    }

    private void sendEventsOverConnection(HttpURLConnection httpCon, byte[] eventsWrapperBytes) throws IOException {
        OutputStream os = httpCon.getOutputStream();
        os.write(eventsWrapperBytes);
        os.flush();
        os.close();
    }

    private byte[] getEventsWrapperBytes(EventsWrapper eventsWrapper) throws JsonProcessingException {
        ObjectMapper om = new ObjectMapper();
        ObjectWriter ow = om.writerFor(om.getTypeFactory().constructType(EventsWrapper.class));
        String eventsWrapperString = ow.writeValueAsString(eventsWrapper);
        return eventsWrapperString.getBytes();
    }

    private HttpURLConnection getHttpURLConnection() throws IOException {
        HttpURLConnection httpURLConnection = (HttpURLConnection) timbermillServerUrl.openConnection();
        httpURLConnection.setRequestMethod("POST");
        httpURLConnection.setRequestProperty("content-type", "application/json");
        httpURLConnection.setDoOutput(true);
        httpURLConnection.setConnectTimeout(HTTP_TIMEOUT);
        httpURLConnection.setReadTimeout(HTTP_TIMEOUT);
        return httpURLConnection;
    }

    private List<Event> getEventsToSend() throws JsonProcessingException{
        long startBatchTime = System.currentTimeMillis();
        List<Event> eventsToSend = new ArrayList<>();
        try {
            int currentBatchSize = addEventFromBufferToList(eventsToSend);
            while(currentBatchSize <= this.maxEventsBatchSize && !isExceededMaxTimeToWait(startBatchTime)) {
                currentBatchSize  += addEventFromBufferToList(eventsToSend);
            }
        } catch (InterruptedException e) {
            // If blocking queue poll timed out send current batch
        }
        return eventsToSend;
    }

    private int addEventFromBufferToList(List<Event> eventsToSend) throws InterruptedException, JsonProcessingException {
        Event event = buffer.poll(maxSecondsBeforeBatchTimeout, TimeUnit.SECONDS);
        if (event == null){
            return 0;
        }
        eventsToSend.add(event);
        return getEventSize(event);
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
            if (!this.thread.isAlive()) {
                thread.start();
            }
        }
    }

    @Override
    public void close() {
    }

    public static class Builder {
        private String timbermillServerUrl;
        private int maxEventsBatchSize = TWO_MB;
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
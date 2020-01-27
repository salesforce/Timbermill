package com.datorama.timbermill.pipe;

import com.datorama.timbermill.unit.Event;
import com.datorama.timbermill.unit.EventsWrapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TimbermillServerOutputPipe implements EventOutputPipe {

    private static final Logger LOG = LoggerFactory.getLogger(TimbermillServerOutputPipe.class);
    private URL timbermillServerUrl;
    private LinkedBlockingQueue<Event> buffer;
    private Thread thread;
    private int maxEventsBatchSize;
    private long maxSecondsBeforeBatchTimeout;


    private TimbermillServerOutputPipe() {
    }

    TimbermillServerOutputPipe(TimbermillServerOutputPipeBuilder builder){
        if (builder.timbermillServerUrl == null){
            throw new RuntimeException("Must enclose the Timbermill server URL");
        }
        try {
            HttpHost httpHost = HttpHost.create(builder.timbermillServerUrl);
            timbermillServerUrl = new URL(httpHost.toURI() + "/events");
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
                        HttpURLConnection httpCon = getHttpURLConnection();
                        byte[] eventsWrapperBytes = getEventsWrapperBytes(eventsWrapper);
                        sendEventsOverConnection(httpCon, eventsWrapperBytes);
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
            } while (true);
        });
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
        HttpURLConnection httpCon = (HttpURLConnection) timbermillServerUrl.openConnection();
        httpCon.setRequestMethod("POST");
        httpCon.setRequestProperty("content-type", "application/json");
        httpCon.setDoOutput(true);
        return httpCon;
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

}
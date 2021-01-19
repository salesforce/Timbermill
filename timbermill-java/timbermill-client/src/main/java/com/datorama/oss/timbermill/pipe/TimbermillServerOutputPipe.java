package com.datorama.oss.timbermill.pipe;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.oss.timbermill.unit.Event;
import com.datorama.oss.timbermill.unit.EventsWrapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class TimbermillServerOutputPipe implements EventOutputPipe {

    private static final int HTTP_TIMEOUT = 10000;
    private static final int MAX_RETRY = 5;
    private static final Logger LOG = LoggerFactory.getLogger(TimbermillServerOutputPipe.class);
    private static volatile boolean keepRunning = true;
    private URL timbermillServerUrl;
    private SizedBoundEventsQueue buffer;
    private ExecutorService executorService;

    private TimbermillServerOutputPipe() {
    }

    TimbermillServerOutputPipe(TimbermillServerOutputPipeBuilder builder){
        keepRunning = true;
        if (builder.timbermillServerUrl == null){
            throw new RuntimeException("Must enclose the Timbermill server URL");
        }
        try {
            HttpHost httpHost = HttpHost.create(builder.timbermillServerUrl);
            timbermillServerUrl = new URL(httpHost.toURI() + "/events");
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
        buffer = new SizedBoundEventsQueue(builder.maxBufferSize, builder.maxSecondsBeforeBatchTimeout);

        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("timbermill-sender-%d").build();
		this.executorService = Executors.newFixedThreadPool(builder.numOfThreads, namedThreadFactory);
        executeEventsSenders(builder.maxEventsBatchSize, builder.numOfThreads);

        // shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        	LOG.info("Shutting down timbermill-senders executor service");
            keepRunning = false;
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                    LOG.info("Events buffer size was {} on shutdown", buffer.size());
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
            }
        }));
    }

    private void executeEventsSenders(int maxEventsBatchSize, int numOfThreads) {

        Runnable getAndSendEventsTask = () -> {
            LOG.info("Starting send events thread");
            do {
                try {
                    List<Event> eventsToSend = buffer.getEventsOfSize(maxEventsBatchSize);
                    if (!eventsToSend.isEmpty()) {
                        EventsWrapper eventsWrapper = new EventsWrapper(eventsToSend);
                        sendEvents(eventsWrapper);
                    }
                } catch (Exception e) {
                    LOG.error("Error sending events to Timbermill server", e);
                }
            } while (keepRunning);
        };

        // execute getAndSendEventsTask multithreaded
        for (int i = 0; i < numOfThreads; i++) {
            executorService.execute(getAndSendEventsTask);
        }
    }

    public void close() {
        LOG.info("Gracefully shutting down Timbermill output pipe.");
        keepRunning = false;
        LOG.info("Timbermill server was output pipe.");
		executorService.shutdown();
        while (!executorService.isTerminated()){
            try {
                Thread.sleep(2000);
            } catch (InterruptedException ignored) {
            }
        }
    }

    private void sendEvents(EventsWrapper eventsWrapper) throws IOException {
        byte[] eventsWrapperBytes = getEventsWrapperBytes(eventsWrapper);
        for (int tryNum = 1; tryNum <= MAX_RETRY; tryNum++) {
            try {
                HttpURLConnection httpCon = getHttpURLConnection();
                sendEventsOverConnection(httpCon, eventsWrapperBytes);
                int responseCode = httpCon.getResponseCode();
                if (responseCode == 200) {
                    LOG.debug("{} events were sent to Timbermill server", eventsWrapper.getEvents().size());
                    return;

                } else {
                    LOG.warn("Request #" + tryNum + " to Timbermill return status {}, Attempt: {}/{} Message: {}", responseCode, tryNum, MAX_RETRY, httpCon.getResponseMessage());
                }
            } catch (Exception e){
                LOG.warn("Request #" + tryNum + " to Timbermill failed, Attempt: "+ tryNum + "/" + MAX_RETRY, e);
            }
            try {
                Thread.sleep((long) (Math.pow(2 , tryNum) * 1000)); //Exponential backoff
            } catch (InterruptedException ignored) {
            }
        }
        LOG.error("Can't send events to Timbermill, failed {} attempts.\n Failed request: {} " , MAX_RETRY, new String(eventsWrapperBytes));
    }

    private void sendEventsOverConnection(HttpURLConnection httpCon, byte[] eventsWrapperBytes) throws IOException {
		try (OutputStream os = httpCon.getOutputStream()) {
            GZIPOutputStream gzipOutputStream = new GZIPOutputStream(os);
            gzipOutputStream.write(eventsWrapperBytes, 0, eventsWrapperBytes.length);
        }
    }

    private byte[] getEventsWrapperBytes(EventsWrapper eventsWrapper) throws JsonProcessingException {
        ObjectMapper om = new ObjectMapper();
        return om.writeValueAsBytes(eventsWrapper);
    }

    private HttpURLConnection getHttpURLConnection() throws IOException {
        HttpURLConnection httpURLConnection = (HttpURLConnection) timbermillServerUrl.openConnection();
        httpURLConnection.setRequestMethod("POST");
        httpURLConnection.setRequestProperty("content-type", "application/json");
        httpURLConnection.setDoOutput(true);
        httpURLConnection.setConnectTimeout(HTTP_TIMEOUT);
        httpURLConnection.setReadTimeout(HTTP_TIMEOUT);
        httpURLConnection.setRequestProperty("Content-Encoding","gzip");
        httpURLConnection.setRequestProperty("Accept-Encoding","gzip");
        return httpURLConnection;
    }

    @Override
    public void send(Event e) {
        if(!this.buffer.offer(e)){
            LOG.warn("Event {} was removed from the queue due to insufficient space", e.getTaskId());
        }
    }

	@Override public int getCurrentBufferSize() {
		return buffer.size();
	}

}
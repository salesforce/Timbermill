package com.datorama.oss.timbermill.pipe;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.oss.timbermill.unit.Event;
import com.datorama.oss.timbermill.unit.EventsWrapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class TimbermillServerOutputPipe implements EventOutputPipe {

    private static final int HTTP_TIMEOUT = 5000;
    private static final int MAX_RETRY = 5;
    private static final Logger LOG = LoggerFactory.getLogger(TimbermillServerOutputPipe.class);
    private static volatile boolean keepRunning = true;
    private URI timbermillServerUri;
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
            timbermillServerUri = new URL(httpHost.toURI() + "/events").toURI();
        } catch (MalformedURLException | URISyntaxException e) {
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
                if (!executorService.awaitTermination(800, TimeUnit.MILLISECONDS)) {
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
            } catch (InterruptedException e) {
            }
        }
    }

    private void sendEvents(EventsWrapper eventsWrapper) throws IOException {
        byte[] eventsWrapperBytes = getEventsWrapperBytes(eventsWrapper);
        for (int tryNum = 1; tryNum <= MAX_RETRY; tryNum++) {
            HttpUriRequest request = generateRequest(eventsWrapperBytes);
            try (CloseableHttpClient httpClient = HttpClients.createDefault();
                    CloseableHttpResponse response = httpClient.execute(request)) {
                int responseCode = response.getStatusLine().getStatusCode();
                if (responseCode == 200) {
                    LOG.debug("{} events were sent to Timbermill server", eventsWrapper.getEvents().size());
                    return;

                } else {
                    LOG.warn("Request #" + tryNum + " to Timbermill return status {}, Attempt: {}/{} Message: {}", responseCode, tryNum, MAX_RETRY, response.getStatusLine().getReasonPhrase());
                }
            } catch (Exception e) {
                LOG.warn("Request #" + tryNum + " to Timbermill failed, Attempt: " + tryNum + "/" + MAX_RETRY, e);
            }
            try {
                Thread.sleep((long) (Math.pow(2, tryNum) * 1000)); //Exponential backoff
            } catch (InterruptedException ignored) {
            }
        }
        LOG.error("Can't send events to Timbermill, failed {} attempts.\n Failed request: {} ", MAX_RETRY, new String(eventsWrapperBytes));
    }

    private byte[] getEventsWrapperBytes(EventsWrapper eventsWrapper) throws JsonProcessingException {
        ObjectMapper om = new ObjectMapper();
        return om.writeValueAsBytes(eventsWrapper);
    }

    private HttpUriRequest generateRequest(byte[] eventsWrapperBytes) {
        HttpPost post = new HttpPost(timbermillServerUri);
        RequestConfig.Builder builder = RequestConfig.custom();
        builder.setConnectTimeout(HTTP_TIMEOUT);
        post.setConfig(builder.build());
        post.setEntity(new ByteArrayEntity(eventsWrapperBytes, ContentType.APPLICATION_JSON));
        return post;
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
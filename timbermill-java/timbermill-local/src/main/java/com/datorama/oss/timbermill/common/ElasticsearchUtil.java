package com.datorama.oss.timbermill.common;

import com.datorama.oss.timbermill.TaskIndexer;
import com.datorama.oss.timbermill.pipe.LocalOutputPipe;
import com.datorama.oss.timbermill.unit.Event;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.datorama.oss.timbermill.TaskIndexer.logErrorInEventsMap;

public class ElasticsearchUtil {
	public static final String CLIENT = "client";
	public static final String PERSISTENCE_HANDLER = "persistence_handler";
	public static final String REDIS_SERVICE = "redis_service";
	public static final String EVENTS_QUEUE = "events_queue";
	public static final String OVERFLOWED_EVENTS_QUEUE = "overflowed_events_queue";
	public static final String RATE_LIMITER_MAP = "rate_limiter_map";
	public static final int THREAD_SLEEP = 2000;
	public static final String SCRIPT =
					  "if (params.orphan != null && !params.orphan) {"
					+ "    ctx._source.orphan = false;"
					+ "}        "
					+ "if (params.dateToDelete != null && !ctx._source.status.equals( \\\"SUCCESS\\\") && !ctx._source.status.equals( \\\"UNTERMINATED\\\") && !ctx._source.status.equals( \\\"ERROR\\\")) {"
					+ "    ctx._source.meta.dateToDelete = params.dateToDelete;"
					+ "}"
					+ "if (params.status != null){"
					+ "        if (ctx._source.string == null){"
					+ "                ctx._source.string =  new HashMap();"
					+ "        }"
					+ "        if (params.status.equals( \\\"CORRUPTED\\\")){"
					+ "                ctx._source.status =  \\\"CORRUPTED\\\" ;"
					+ "        }"
					+ "        else if (ctx._source.status.equals( \\\"SUCCESS\\\") || ctx._source.status.equals( \\\"ERROR\\\" )){"
					+ "            if(params.status.equals( \\\"SUCCESS\\\") || params.status.equals( \\\"ERROR\\\" )){"
					+ "                 if(!ctx._source.status.equals(params.status)){"
					+ "                     ctx._source.status =  \\\"CORRUPTED\\\" ;"
					+ "                     ctx._source.string.put(\\\"corruptedReason\\\",\\\"ALREADY_CLOSED_DIFFERENT_CLOSE_STATUS\\\");"
					+ "                 }"
					+ "                 else if(!ctx._source.meta.taskEnd.equals(params.taskEnd)){"
					+ "                     ctx._source.status =  \\\"CORRUPTED\\\" ;"
					+ "                     ctx._source.string.put(\\\"corruptedReason\\\",\\\"ALREADY_CLOSED_DIFFERENT_CLOSE_TIME\\\");"
					+ "                 }"
					+ "                 else if(!ctx._source.meta.taskBegin.equals(params.taskBegin)){"
					+ "                     ctx._source.status =  \\\"CORRUPTED\\\" ;"
					+ "                     ctx._source.string.put(\\\"corruptedReason\\\",\\\"ALREADY_STARTED_DIFFERENT_START_TIME\\\");"
					+ "                 }"
					+ "            }"
					+ "            else if (params.status.equals( \\\"UNTERMINATED\\\")){"
					+ "                 if(!ctx._source.meta.taskBegin.equals(params.taskBegin)){"
					+ "                     ctx._source.status =  \\\"CORRUPTED\\\" ;"
					+ "                     ctx._source.string.put(\\\"corruptedReason\\\",\\\"ALREADY_STARTED_DIFFERENT_START_TIME\\\");"
					+ "                 }"
					+ "            }"
					+ "            else if (params.status.equals( \\\"PARTIAL_SUCCESS\\\")){"
					+ "                 if(ctx._source.status.equals( \\\"ERROR\\\" )){"
					+ "                     ctx._source.status =  \\\"CORRUPTED\\\" ;"
					+ "                     ctx._source.string.put(\\\"corruptedReason\\\",\\\"ALREADY_CLOSED_DIFFERENT_CLOSE_STATUS\\\");"
					+ "                 }"
					+ "                 else if(!ctx._source.meta.taskEnd.equals(params.taskEnd)){"
					+ "                     ctx._source.status =  \\\"CORRUPTED\\\" ;"
					+ "                     ctx._source.string.put(\\\"corruptedReason\\\",\\\"ALREADY_CLOSED_DIFFERENT_CLOSE_TIME\\\");"
					+ "                 }"
					+ "            }"
					+ "            else if (params.status.equals( \\\"PARTIAL_ERROR\\\")){"
					+ "                 if(ctx._source.status.equals( \\\"SUCCESS\\\" )){"
					+ "                     ctx._source.status =  \\\"CORRUPTED\\\" ;"
					+ "                     ctx._source.string.put(\\\"corruptedReason\\\",\\\"ALREADY_CLOSED_DIFFERENT_CLOSE_STATUS\\\");"
					+ "                 }"
					+ "                 else if(!ctx._source.meta.taskEnd.equals(params.taskEnd)){"
					+ "                     ctx._source.status =  \\\"CORRUPTED\\\" ;"
					+ "                     ctx._source.string.put(\\\"corruptedReason\\\",\\\"ALREADY_CLOSED_DIFFERENT_CLOSE_TIME\\\");"
					+ "                 }"
					+ "            }"
					+ "        }"
					+ "        else if (ctx._source.status.equals( \\\"UNTERMINATED\\\")){"
					+ "            if(params.status.equals( \\\"SUCCESS\\\" ) || params.status.equals( \\\"ERROR\\\" ) || params.status.equals( \\\"UNTERMINATED\\\")){"
					+ "                 if(!ctx._source.meta.taskBegin.equals(params.taskBegin)){"
					+ "                     ctx._source.status =  \\\"CORRUPTED\\\" ;"
					+ "                     ctx._source.string.put(\\\"corruptedReason\\\",\\\"ALREADY_STARTED_DIFFERENT_START_TIME\\\");"
					+ "                 }"
					+ "            }"
					+ "            else if (params.status.equals( \\\"PARTIAL_SUCCESS\\\")){"
					+ "                long taskBegin = ZonedDateTime.parse(ctx._source.meta.taskBegin, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant().toEpochMilli();"
					+ "                ctx._source.meta.duration = params.taskEndMillis - taskBegin;"
					+ "                ctx._source.meta.taskEnd = params.taskEnd;"
					+ "                ctx._source.status =  \\\"SUCCESS\\\" ;"
					+ "            }"
					+ "            else if (params.status.equals( \\\"PARTIAL_ERROR\\\")){"
					+ "                long taskBegin = ZonedDateTime.parse(ctx._source.meta.taskBegin, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant().toEpochMilli();"
					+ "                ctx._source.meta.duration = params.taskEndMillis - taskBegin;"
					+ "                ctx._source.meta.taskEnd = params.taskEnd;"
					+ "                ctx._source.status = \\\"ERROR\\\";"
					+ "            }"
					+ "        }"
					+ "        else if (ctx._source.status.equals( \\\"PARTIAL_SUCCESS\\\")){"
					+ "            if(params.status.equals( \\\"SUCCESS\\\" ) || params.status.equals( \\\"PARTIAL_SUCCESS\\\")){"
					+ "                 if(!ctx._source.meta.taskEnd.equals(params.taskEnd)){"
					+ "                     ctx._source.status =  \\\"CORRUPTED\\\" ;"
					+ "                     ctx._source.string.put(\\\"corruptedReason\\\",\\\"ALREADY_CLOSED_DIFFERENT_CLOSE_TIME\\\");"
					+ "                 }"
					+ "            }"
					+ "            else if(params.status.equals( \\\"ERROR\\\") || params.status.equals( \\\"PARTIAL_ERROR\\\")){"
					+ "                 if(!ctx._source.status.equals(params.status)){"
					+ "                     ctx._source.status =  \\\"CORRUPTED\\\" ;"
					+ "                     ctx._source.string.put(\\\"corruptedReason\\\",\\\"ALREADY_CLOSED_DIFFERENT_CLOSE_STATUS\\\");"
					+ "                 }"
					+ "            }"
					+ "            else if (params.status.equals( \\\"UNTERMINATED\\\")){"
					+ "                long taskEnd = ZonedDateTime.parse(ctx._source.meta.taskEnd, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant().toEpochMilli();"
					+ "                ctx._source.meta.duration = taskEnd - params.taskBeginMillis;"
					+ "                ctx._source.meta.taskBegin = params.taskBegin;"
					+ "                ctx._source.status =  \\\"SUCCESS\\\" ;"
					+ "            }"
					+ "        }"
					+ "        else if (ctx._source.status.equals( \\\"PARTIAL_ERROR\\\")){"
					+ "            if(params.status.equals( \\\"SUCCESS\\\" ) || params.status.equals( \\\"PARTIAL_SUCCESS\\\")){"
					+ "                 ctx._source.status =  \\\"CORRUPTED\\\" ;"
					+ "                 ctx._source.string.put(\\\"corruptedReason\\\",\\\"ALREADY_CLOSED_DIFFERENT_CLOSE_STATUS\\\");"
					+ "            }"
					+ "            else if(params.status.equals( \\\"ERROR\\\") || params.status.equals( \\\"PARTIAL_ERROR\\\")){"
					+ "                 if(!ctx._source.meta.taskEnd.equals(params.taskEnd)){"
					+ "                     ctx._source.status =  \\\"CORRUPTED\\\" ;"
					+ "                     ctx._source.string.put(\\\"corruptedReason\\\",\\\"ALREADY_CLOSED_DIFFERENT_CLOSE_TIME\\\");"
					+ "                 }"
					+ "            }"
					+ "            else if (params.status.equals( \\\"UNTERMINATED\\\")){"
					+ "                long taskEnd = ZonedDateTime.parse(ctx._source.meta.taskEnd, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant().toEpochMilli();"
					+ "                ctx._source.meta.duration = taskEnd - params.taskBeginMillis;"
					+ "                ctx._source.meta.taskBegin = params.taskBegin;"
					+ "                ctx._source.status =  \\\"ERROR\\\" ;"
					+ "            }"
					+ "            else if (params.status.equals( \\\"CORRUPTED\\\")){"
					+ "                ctx._source.status =  \\\"CORRUPTED\\\" ;"
					+ "            }"
					+ "        }"
					+ "        else if (ctx._source.status.equals( \\\"PARTIAL_INFO_ONLY\\\")){"
					+ "            if(params.status.equals( \\\"SUCCESS\\\" ) || params.status.equals( \\\"ERROR\\\" )){"
					+ "                ctx._source.meta.duration = params.taskEndMillis - params.taskBeginMillis;"
					+ "                ctx._source.meta.taskEnd = params.taskEnd;"
					+ "                ctx._source.meta.taskBegin = params.taskBegin;"
					+ "                ctx._source.status = params.status;"
					+ "            }"
					+ "            else if (params.status.equals( \\\"UNTERMINATED\\\")){"
					+ "                ctx._source.meta.taskBegin = params.taskBegin;"
					+ "                ctx._source.status = params.status;"
					+ "            }"
					+ "            else if (params.status.equals( \\\"PARTIAL_SUCCESS\\\")){"
					+ "                ctx._source.meta.taskEnd = params.taskEnd;"
					+ "                ctx._source.status = params.status;"
					+ "            }"
					+ "            else if (params.status.equals( \\\"PARTIAL_ERROR\\\")){"
					+ "                ctx._source.meta.taskEnd = params.taskEnd;"
					+ "                ctx._source.status = params.status;"
					+ "            }"
					+ "        }"
					+ "        else {"
					+ "            ctx._source.status =  \\\"CORRUPTED\\\" ;"
					+ "        }"
					+ "}"
					+ "     if (params.contx != null) {"
					+ "            if (ctx._source.ctx == null) {"
					+ "                ctx._source.ctx = params.contx;"
					+ "            }"
					+ "            else {"
					+ "                ctx._source.ctx.putAll(params.contx);"
					+ "            }"
					+ "        }"
					+ "        if (params.string != null) {"
					+ "            if (ctx._source.string == null) {"
					+ "                ctx._source.string = params.string;"
					+ "            }"
					+ "            else {"
					+ "                ctx._source.string.putAll(params.string);"
					+ "            }"
					+ "        }"
					+ "        if (params.text != null) {"
					+ "            if (ctx._source.text == null) {"
					+ "                ctx._source.text = params.text;"
					+ "            }"
					+ "            else {"
					+ "                ctx._source.text.putAll(params.text);"
					+ "            }"
					+ "        }"
					+ "        if (params.metric != null) {"
					+ "            if (ctx._source.metric == null) {"
					+ "                ctx._source.metric = params.metric;"
					+ "            }"
					+ "            else {"
					+ "                ctx._source.metric.putAll(params.metric);"
					+ "            }"
					+ "        }"
					+ "        if (params.name != null) {"
					+ "            ctx._source.name = params.name;"
					+ "        }"
					+ "        if (params.parentId != null) {"
					+ "            ctx._source.parentId = params.parentId;"
					+ "        }"
					+ "        if (params.primaryId != null) {"
					+ "            ctx._source.primaryId = params.primaryId;"
					+ "        }"
					+ "        if (params.parentsPath != null) {"
					+ "            ctx._source.parentsPath = params.parentsPath;"
					+ "        }"
					+ "        if (params.orphan != null && params.orphan) {"
					+ "            ctx._source.orphan = true;"
					+ "        }\"\n";

	public static final String MAPPING = "   {\"dynamic_templates\": [\n"
			+ "      {\n"
			+ "        \"env\": {\n"
			+ "          \"path_match\":   \"env\",\n"
			+ "          \"mapping\": {\n"
			+ "            \"type\":       \"keyword\"\n"
			+ "          }\n"
			+ "        }\n"
			+ "      },\n"
			+ "            {\n"
			+ "        \"name\": {\n"
			+ "          \"path_match\":   \"name\",\n"
			+ "          \"mapping\": {\n"
			+ "            \"type\":       \"keyword\"\n"
			+ "          }\n"
			+ "        }\n"
			+ "      },\n"
			+ "            {\n"
			+ "        \"status\": {\n"
			+ "          \"path_match\":   \"status\",\n"
			+ "          \"mapping\": {\n"
			+ "            \"type\":       \"keyword\"\n"
			+ "          }\n"
			+ "        }\n"
			+ "      },\n"
			+ "            {\n"
			+ "        \"parentId\": {\n"
			+ "          \"path_match\":   \"parentId\",\n"
			+ "          \"mapping\": {\n"
			+ "            \"type\":       \"keyword\"\n"
			+ "          }\n"
			+ "        }\n"
			+ "      },\n"
			+ "            {\n"
			+ "        \"primaryId\": {\n"
			+ "          \"path_match\":   \"primaryId\",\n"
			+ "          \"mapping\": {\n"
			+ "            \"type\":       \"keyword\"\n"
			+ "          }\n"
			+ "        }\n"
			+ "      },\n"
			+ "                  {\n"
			+ "        \"log\": {\n"
			+ "          \"path_match\":   \"log\",\n"
			+ "          \"mapping\": {\n"
			+ "            \"type\":       \"text\"\n"
			+ "          }\n"
			+ "        }\n"
			+ "      },\n"
			+ "      {\n"
			+ "        \"context\": {\n"
			+ "          \"path_match\":   \"ctx.*\",\n"
			+ "          \"mapping\": {\n"
			+ "            \"type\":       \"keyword\"\n"
			+ "          }\n"
			+ "        }\n"
			+ "      },\n"
			+ "      {\n"
			+ "        \"string\": {\n"
			+ "          \"path_match\":   \"string.*\",\n"
			+ "          \"mapping\": {\n"
			+ "            \"type\":       \"keyword\"\n"
			+ "          }\n"
			+ "        }\n"
			+ "      },\n"
			+ "      {\n"
			+ "        \"text\": {\n"
			+ "          \"path_match\":   \"text.*\",\n"
			+ "          \"mapping\": {\n"
			+ "            \"type\":       \"text\"\n"
			+ "          }\n"
			+ "        }\n"
			+ "      },\n"
			+ "            {\n"
			+ "        \"metric\": {\n"
			+ "          \"path_match\":   \"metric.*\",\n"
			+ "          \"mapping\": {\n"
			+ "            \"type\":       \"double\"\n"
			+ "          }\n"
			+ "        }\n"
			+ "      }\n"
			+ "    ]\n"
			+ "  }\n"
			+ "}";

	private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchUtil.class);

	public static final String TIMBERMILL_INDEX_PREFIX = "timbermill2";
	public static final String TIMBERMILL_INDEX_WILDCARD = TIMBERMILL_INDEX_PREFIX + "*";
	public static final String INDEX_DELIMITER = "-";
	public static final String OLD_SUFFIX = "old";

	private static final Set<String> envsSet = Sets.newConcurrentHashSet();
	private static final Pattern metadataPatten = Pattern.compile("metadata.*");
	private static Pattern notToSkipRegexPattern;

	public static Set<String> getEnvSet() {
		return envsSet;
	}

	public static void drainAndIndex(BlockingQueue<Event> eventsQueue, TaskIndexer taskIndexer, int maxElement) {
		drainAndIndex(eventsQueue, taskIndexer, maxElement, false, ".*");
	}

	public static void drainAndIndex(BlockingQueue<Event> eventsQueue, TaskIndexer taskIndexer, int maxElement, boolean skipEventsAtDrainFlag, String notToSkipRegex) {
		while (!eventsQueue.isEmpty()) {
			try {
				Collection<Event> unfilteredEvents = new ArrayList<>();
				eventsQueue.drainTo(unfilteredEvents, maxElement);
				Collection<Event> events = filterEvents(unfilteredEvents, skipEventsAtDrainFlag, notToSkipRegex);

				if (LocalOutputPipe.getClientFacingEventsPattern() != null){
					events.forEach(e -> {
						if (LocalOutputPipe.getClientFacingEventsPattern().matcher(e.getName()).matches()){
							KamonConstants.MESSAGES_IN_INPUT_QUEUE_RANGE_SAMPLER.withTag("client_facing", true).decrement();
						} else {
							KamonConstants.MESSAGES_IN_INPUT_QUEUE_RANGE_SAMPLER.withTag("client_facing", false).decrement();
						}
					});
				} else {
					KamonConstants.MESSAGES_IN_INPUT_QUEUE_RANGE_SAMPLER.withoutTags().decrement(events.size());
				}
				logErrorInEventsMap(events.stream().filter(event -> event.getTaskId() != null).collect(Collectors.groupingBy(Event::getTaskId)), "drainAndIndex");

				events.forEach(e -> {
					if (e.getEnv() == null){
						e.setEnv(Constants.DEFAULT);
					}
				});

				Map<String, List<Event>> eventsPerEnvMap = events.stream().collect(Collectors.groupingBy(Event::getEnv));
				for (Map.Entry<String, List<Event>> eventsPerEnv : eventsPerEnvMap.entrySet()) {
					String env = eventsPerEnv.getKey().toLowerCase();

					envsSet.add(env);

					Collection<Event> currentEvents = eventsPerEnv.getValue();
					taskIndexer.retrieveAndIndex(currentEvents, env);
				}
				//For refresh
				try {
					Thread.sleep(THREAD_SLEEP);
				} catch (InterruptedException e) {
					LOG.error("InterruptedException was thrown from TaskIndexer:", e);
				}
			} catch (NullPointerException e) {
//				LOG.error("NullPointerException was thrown from TaskIndexer:{}\n {}", e.getMessage(), e.getStackTrace());
				LOG.error("NullPointerException was thrown from TaskIndexer", e);

			} catch (RuntimeException e) {
				LOG.error("Error was thrown from TaskIndexer:", e);
			}
		}
	}

	private static Collection<Event> filterEvents(Collection<Event> unfilteredEvents, boolean skipEventsAtDrainFlag, String notToSkipRegex) {
		if (skipEventsAtDrainFlag) {
			return unfilteredEvents.stream()
					.filter(event-> shouldKeep(event.getName(), event.getTaskId(), notToSkipRegex))
					.collect(Collectors.toList());
		}
		return unfilteredEvents;
	}

	private static boolean shouldKeep(String eventName, String eventId, String notToSkipRegex) {
		if (metadataPatten.matcher(eventName).matches()) {
			return true;
		}
		if (notToSkipRegexPattern == null) {
			notToSkipRegexPattern = Pattern.compile(notToSkipRegex);
		}
		boolean match = notToSkipRegexPattern.matcher(eventName).matches();
		if (match) {
			LOG.info("skipEvents | keeping task {} task id: {} at drain", eventName, eventId);
			return true;
		}
		LOG.debug("skipEvents | skipping task {} task id: {} at drain", eventName, eventId);
		return false;
	}

	public static long getTimesDuration(ZonedDateTime taskIndexerStartTime, ZonedDateTime taskIndexerEndTime) {
		return ChronoUnit.MILLIS.between(taskIndexerStartTime, taskIndexerEndTime);
	}

	public static String getTimbermillIndexAlias(String env) {
		return TIMBERMILL_INDEX_PREFIX + INDEX_DELIMITER + env;
	}

	public static String getOldAlias(String currentAlias) {
		return currentAlias + INDEX_DELIMITER + OLD_SUFFIX;
	}

	public static String getIndexSerial(int serialNumber) {
		return String.format("%06d", serialNumber);
	}
}

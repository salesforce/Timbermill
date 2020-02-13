package com.datorama.oss.timbermill.cron;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.impl.JobDetailImpl;
import org.quartz.impl.JobExecutionContextImpl;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.TriggerFiredBundle;

import com.datorama.oss.timbermill.ElasticsearchClient;
import com.datorama.oss.timbermill.TimberLogTest;
import com.datorama.oss.timbermill.unit.*;
import com.google.common.collect.Lists;

import static com.datorama.oss.timbermill.common.ElasticsearchUtil.CLIENT;
import static org.junit.Assert.*;

public class TasksMergerJobsTest extends TimberLogTest {

	private static final String CTX_1 = "ctx1";
	private static final String CTX_2 = "ctx2";
	private static final String CTX_3 = "ctx3";
	private static final String METRIC_1 = "metric1";
	private static final String METRIC_2 = "metric2";
	private static final String METRIC_3 = "metric3";
	private static final String TEXT_1 = "text1";
	private static final String TEXT_2 = "text2";
	private static final String TEXT_3 = "text3";
	private static final String STRING_1 = "string1";
	private static final String STRING_2 = "string2";
	private static final String STRING_3 = "string3";
	private static final String ROLLOVER_TEST = "rollover_test";
	private static JobExecutionContextImpl context;
	private static TasksMergerJobs tasksMergerJobs;

	@BeforeClass
	public static void init() {
		String elasticUrl = System.getenv("ELASTICSEARCH_URL");
		if (StringUtils.isEmpty(elasticUrl)){
			elasticUrl = DEFAULT_ELASTICSEARCH_URL;
		}
		TimberLogTest.client =  new ElasticsearchClient(elasticUrl, 1000, 1, null, null, null, 7, 100, 1000000000, 3, 3);
		tasksMergerJobs = new TasksMergerJobs();
		JobDetail job = new JobDetailImpl();
		JobDataMap jobDataMap = job.getJobDataMap();
		String currentIndex = "com.datorama.oss.timbermill-test-new";
		String previousIndex = "com.datorama.oss.timbermill-test-old";
		TimberLogTest.client.setOldIndex(previousIndex);
		TimberLogTest.client.setCurrentIndex(currentIndex);
		jobDataMap.put(CLIENT, TimberLogTest.client);
		OperableTrigger trigger = new SimpleTriggerImpl();
		TriggerFiredBundle fireBundle = new TriggerFiredBundle(job, trigger, null, true, null, null, null, null);
		context = new JobExecutionContextImpl(null, fireBundle, null);
	}

	@Test
	public void testStartSuccessDifferentIndex() throws InterruptedException {
		Map<String, Task> newTasks = new HashMap<>();
		Map<String, Task> oldTasks = new HashMap<>();
		String id = Event.generateTaskId(ROLLOVER_TEST);

		LogParams startLogParams = LogParams.create().context(CTX_1, CTX_1).metric(METRIC_1,1).text(TEXT_1, TEXT_1).string(STRING_1, STRING_1);
		Event startEvent = new StartEvent(id, ROLLOVER_TEST, startLogParams, ROLLOVER_TEST, null);
		List<Event> oldEvents = Lists.newArrayList(startEvent);
		Task oldTask = new Task(oldEvents, 1);
		oldTasks.put(id, oldTask);

		Thread.sleep(10);

		LogParams successLogParams = LogParams.create().context(CTX_2, CTX_2).metric(METRIC_2,2).text(TEXT_2, TEXT_2).string(STRING_2, STRING_2);
		Event successEvent = new SuccessEvent(id, successLogParams);
		List<Event> newEvents = Lists.newArrayList(successEvent);
		Task newTask = new Task(newEvents, 1);
		newTasks.put(id, newTask);


		TimberLogTest.client.index(oldTasks, TimberLogTest.client.getOldIndex());
		TimberLogTest.waitForTask(id, TaskStatus.UNTERMINATED);
		TimberLogTest.client.index(newTasks, TimberLogTest.client.getCurrentIndex());
		TimberLogTest.waitForTasks(id, 2);
		tasksMergerJobs.execute(context);

		TimberLogTest.waitForTask(id, TaskStatus.SUCCESS);
		Task task = TimberLogTest.client.getTaskById(id);

		assertNotNull(task);
		assertEquals(TaskStatus.SUCCESS, task.getStatus());
		assertEquals(CTX_1, task.getCtx().get(CTX_1));
		assertEquals(CTX_2, task.getCtx().get(CTX_2));
		assertEquals(1, task.getMetric().get(METRIC_1).intValue());
		assertEquals(2, task.getMetric().get(METRIC_2).intValue());
		assertEquals(TEXT_1, task.getText().get(TEXT_1));
		assertEquals(TEXT_2, task.getText().get(TEXT_2));
		assertEquals(STRING_1, task.getString().get(STRING_1));
		assertEquals(STRING_2, task.getString().get(STRING_2));
		assertNotEquals((Long) 0L, task.getDuration());
		assertNotEquals(task.getEndTime(), task.getStartTime());
	}

	@Test
	public void testStartInfoSuccessDifferentIndex() throws InterruptedException {
		Map<String, Task> newTasks = new HashMap<>();
		Map<String, Task> oldTasks = new HashMap<>();
		String id = Event.generateTaskId(ROLLOVER_TEST);

		LogParams startLogParams = LogParams.create().context(CTX_1, CTX_1).metric(METRIC_1,1).text(TEXT_1, TEXT_1).string(STRING_1, STRING_1);
		Event startEvent = new StartEvent(id, ROLLOVER_TEST, startLogParams, ROLLOVER_TEST, null);
		LogParams infoLogParams = LogParams.create().context(CTX_3, CTX_3).metric(METRIC_3,3).text(TEXT_3, TEXT_3).string(STRING_3, STRING_3);
		InfoEvent infoEvent = new InfoEvent(id, infoLogParams);
		List<Event> oldEvents = Lists.newArrayList(startEvent, infoEvent);
		Task oldTask = new Task(oldEvents, 1);
		oldTasks.put(id, oldTask);

		Thread.sleep(10);

		LogParams successLogParams = LogParams.create().context(CTX_2, CTX_2).metric(METRIC_2,2).text(TEXT_2, TEXT_2).string(STRING_2, STRING_2);
		Event successEvent = new SuccessEvent(id, successLogParams);
		List<Event> newEvents = Lists.newArrayList(successEvent);
		Task newTask = new Task(newEvents, 1);
		newTasks.put(id, newTask);


		TimberLogTest.client.index(oldTasks, TimberLogTest.client.getOldIndex());
		TimberLogTest.waitForTask(id, TaskStatus.UNTERMINATED);
		TimberLogTest.client.index(newTasks, TimberLogTest.client.getCurrentIndex());
		TimberLogTest.waitForTasks(id, 2);
		tasksMergerJobs.execute(context);

		TimberLogTest.waitForTask(id, TaskStatus.SUCCESS);
		Task task = TimberLogTest.client.getTaskById(id);

		assertNotNull(task);
		assertEquals(TaskStatus.SUCCESS, task.getStatus());
		assertEquals(CTX_1, task.getCtx().get(CTX_1));
		assertEquals(CTX_2, task.getCtx().get(CTX_2));
		assertEquals(CTX_3, task.getCtx().get(CTX_3));
		assertEquals(1, task.getMetric().get(METRIC_1).intValue());
		assertEquals(2, task.getMetric().get(METRIC_2).intValue());
		assertEquals(3, task.getMetric().get(METRIC_3).intValue());
		assertEquals(TEXT_1, task.getText().get(TEXT_1));
		assertEquals(TEXT_2, task.getText().get(TEXT_2));
		assertEquals(TEXT_3, task.getText().get(TEXT_3));
		assertEquals(STRING_1, task.getString().get(STRING_1));
		assertEquals(STRING_2, task.getString().get(STRING_2));
		assertEquals(STRING_3, task.getString().get(STRING_3));
		assertNotEquals((Long) 0L, task.getDuration());
		assertNotEquals(task.getEndTime(), task.getStartTime());
	}

	@Test
	public void testSuccessStartDifferentIndex() throws InterruptedException {
		Map<String, Task> newTasks = new HashMap<>();
		Map<String, Task> oldTasks = new HashMap<>();
		String id = Event.generateTaskId(ROLLOVER_TEST);

		LogParams successLogParams = LogParams.create().context(CTX_2, CTX_2).metric(METRIC_2,2).text(TEXT_2, TEXT_2).string(STRING_2, STRING_2);
		Event successEvent = new SuccessEvent(id, successLogParams);
		List<Event> oldEvents = Lists.newArrayList(successEvent);
		Task oldTask = new Task(oldEvents, 1);
		oldTasks.put(id, oldTask);

		Thread.sleep(10);

		LogParams startLogParams = LogParams.create().context(CTX_1, CTX_1).metric(METRIC_1,1).text(TEXT_1, TEXT_1).string(STRING_1, STRING_1);
		Event startEvent = new StartEvent(id, ROLLOVER_TEST, startLogParams, ROLLOVER_TEST, null);
		List<Event> newEvents = Lists.newArrayList(startEvent);
		Task newTask = new Task(newEvents, 1);
		newTasks.put(id, newTask);


		TimberLogTest.client.index(oldTasks, TimberLogTest.client.getOldIndex());
		TimberLogTest.waitForTask(id, TaskStatus.PARTIAL_SUCCESS);
		TimberLogTest.client.index(newTasks, TimberLogTest.client.getCurrentIndex());
		TimberLogTest.waitForTasks(id, 2);
		tasksMergerJobs.execute(context);

		TimberLogTest.waitForTask(id, TaskStatus.SUCCESS);
		Task task = TimberLogTest.client.getTaskById(id);

		assertNotNull(task);
		assertEquals(TaskStatus.SUCCESS, task.getStatus());
		assertEquals(CTX_1, task.getCtx().get(CTX_1));
		assertEquals(CTX_2, task.getCtx().get(CTX_2));
		assertEquals(1, task.getMetric().get(METRIC_1).intValue());
		assertEquals(2, task.getMetric().get(METRIC_2).intValue());
		assertEquals(TEXT_1, task.getText().get(TEXT_1));
		assertEquals(TEXT_2, task.getText().get(TEXT_2));
		assertEquals(STRING_1, task.getString().get(STRING_1));
		assertEquals(STRING_2, task.getString().get(STRING_2));
		assertNotEquals((Long) 0L, task.getDuration());
		assertNotEquals(task.getEndTime(), task.getStartTime());
	}

	@Test
	public void testStartErrorDifferentIndex() throws InterruptedException {
		Map<String, Task> newTasks = new HashMap<>();
		Map<String, Task> oldTasks = new HashMap<>();
		String id = Event.generateTaskId(ROLLOVER_TEST);

		LogParams startLogParams = LogParams.create().context(CTX_1, CTX_1).metric(METRIC_1,1).text(TEXT_1, TEXT_1).string(STRING_1, STRING_1);
		Event startEvent = new StartEvent(id, ROLLOVER_TEST, startLogParams, ROLLOVER_TEST, null);
		List<Event> oldEvents = Lists.newArrayList(startEvent);
		Task oldTask = new Task(oldEvents, 1);
		oldTasks.put(id, oldTask);

		Thread.sleep(10);

		LogParams errorLogParams = LogParams.create().context(CTX_2, CTX_2).metric(METRIC_2,2).text(TEXT_2, TEXT_2).string(STRING_2, STRING_2);
		Event errorEvent = new ErrorEvent(id, errorLogParams);
		List<Event> newEvents = Lists.newArrayList(errorEvent);
		Task newTask = new Task(newEvents, 1);
		newTasks.put(id, newTask);


		TimberLogTest.client.index(oldTasks, TimberLogTest.client.getOldIndex());
		TimberLogTest.waitForTask(id, TaskStatus.UNTERMINATED);
		TimberLogTest.client.index(newTasks, TimberLogTest.client.getCurrentIndex());
		TimberLogTest.waitForTasks(id, 2);
		tasksMergerJobs.execute(context);

		TimberLogTest.waitForTask(id, TaskStatus.ERROR);
		Task task = TimberLogTest.client.getTaskById(id);

		assertNotNull(task);
		assertEquals(TaskStatus.ERROR, task.getStatus());
		assertEquals(CTX_1, task.getCtx().get(CTX_1));
		assertEquals(CTX_2, task.getCtx().get(CTX_2));
		assertEquals(1, task.getMetric().get(METRIC_1).intValue());
		assertEquals(2, task.getMetric().get(METRIC_2).intValue());
		assertEquals(TEXT_1, task.getText().get(TEXT_1));
		assertEquals(TEXT_2, task.getText().get(TEXT_2));
		assertEquals(STRING_1, task.getString().get(STRING_1));
		assertEquals(STRING_2, task.getString().get(STRING_2));
		assertNotEquals((Long) 0L, task.getDuration());
		assertNotEquals(task.getEndTime(), task.getStartTime());
	}

	@Test
	public void testStartInfoErrorDifferentIndex() throws InterruptedException {
		Map<String, Task> newTasks = new HashMap<>();
		Map<String, Task> oldTasks = new HashMap<>();
		String id = Event.generateTaskId(ROLLOVER_TEST);

		LogParams startLogParams = LogParams.create().context(CTX_1, CTX_1).metric(METRIC_1,1).text(TEXT_1, TEXT_1).string(STRING_1, STRING_1);
		Event startEvent = new StartEvent(id, ROLLOVER_TEST, startLogParams, ROLLOVER_TEST, null);
		LogParams infoLogParams = LogParams.create().context(CTX_3, CTX_3).metric(METRIC_3,3).text(TEXT_3, TEXT_3).string(STRING_3, STRING_3);
		InfoEvent infoEvent = new InfoEvent(id, infoLogParams);
		List<Event> oldEvents = Lists.newArrayList(startEvent, infoEvent);
		Task oldTask = new Task(oldEvents, 1);
		oldTasks.put(id, oldTask);

		Thread.sleep(10);

		LogParams errorLogParams = LogParams.create().context(CTX_2, CTX_2).metric(METRIC_2,2).text(TEXT_2, TEXT_2).string(STRING_2, STRING_2);
		Event successEvent = new ErrorEvent(id, errorLogParams);
		List<Event> newEvents = Lists.newArrayList(successEvent);
		Task newTask = new Task(newEvents, 1);
		newTasks.put(id, newTask);


		TimberLogTest.client.index(oldTasks, TimberLogTest.client.getOldIndex());
		TimberLogTest.waitForTask(id, TaskStatus.UNTERMINATED);
		TimberLogTest.client.index(newTasks, TimberLogTest.client.getCurrentIndex());
		TimberLogTest.waitForTasks(id, 2);
		tasksMergerJobs.execute(context);

		TimberLogTest.waitForTask(id, TaskStatus.ERROR);
		Task task = TimberLogTest.client.getTaskById(id);

		assertNotNull(task);
		assertEquals(TaskStatus.ERROR, task.getStatus());
		assertEquals(CTX_1, task.getCtx().get(CTX_1));
		assertEquals(CTX_2, task.getCtx().get(CTX_2));
		assertEquals(CTX_3, task.getCtx().get(CTX_3));
		assertEquals(1, task.getMetric().get(METRIC_1).intValue());
		assertEquals(2, task.getMetric().get(METRIC_2).intValue());
		assertEquals(3, task.getMetric().get(METRIC_3).intValue());
		assertEquals(TEXT_1, task.getText().get(TEXT_1));
		assertEquals(TEXT_2, task.getText().get(TEXT_2));
		assertEquals(TEXT_3, task.getText().get(TEXT_3));
		assertEquals(STRING_1, task.getString().get(STRING_1));
		assertEquals(STRING_2, task.getString().get(STRING_2));
		assertEquals(STRING_3, task.getString().get(STRING_3));
		assertNotEquals((Long) 0L, task.getDuration());
		assertNotEquals(task.getEndTime(), task.getStartTime());
	}

	@Test
	public void testErrorStartDifferentIndex() throws InterruptedException {
		Map<String, Task> newTasks = new HashMap<>();
		Map<String, Task> oldTasks = new HashMap<>();
		String id = Event.generateTaskId(ROLLOVER_TEST);

		LogParams errorLogParams = LogParams.create().context(CTX_2, CTX_2).metric(METRIC_2,2).text(TEXT_2, TEXT_2).string(STRING_2, STRING_2);
		Event errorEvent = new ErrorEvent(id, errorLogParams);
		List<Event> oldEvents = Lists.newArrayList(errorEvent);
		Task oldTask = new Task(oldEvents, 1);
		oldTasks.put(id, oldTask);

		Thread.sleep(10);

		LogParams startLogParams = LogParams.create().context(CTX_1, CTX_1).metric(METRIC_1,1).text(TEXT_1, TEXT_1).string(STRING_1, STRING_1);
		Event startEvent = new StartEvent(id, ROLLOVER_TEST, startLogParams, ROLLOVER_TEST, null);
		List<Event> newEvents = Lists.newArrayList(startEvent);
		Task newTask = new Task(newEvents, 1);
		newTasks.put(id, newTask);


		TimberLogTest.client.index(oldTasks, TimberLogTest.client.getOldIndex());
		TimberLogTest.waitForTask(id, TaskStatus.PARTIAL_ERROR);
		TimberLogTest.client.index(newTasks, TimberLogTest.client.getCurrentIndex());
		TimberLogTest.waitForTasks(id, 2);
		tasksMergerJobs.execute(context);

		TimberLogTest.waitForTask(id, TaskStatus.ERROR);
		Task task = TimberLogTest.client.getTaskById(id);

		assertNotNull(task);
		assertEquals(TaskStatus.ERROR, task.getStatus());
		assertEquals(CTX_1, task.getCtx().get(CTX_1));
		assertEquals(CTX_2, task.getCtx().get(CTX_2));
		assertEquals(1, task.getMetric().get(METRIC_1).intValue());
		assertEquals(2, task.getMetric().get(METRIC_2).intValue());
		assertEquals(TEXT_1, task.getText().get(TEXT_1));
		assertEquals(TEXT_2, task.getText().get(TEXT_2));
		assertEquals(STRING_1, task.getString().get(STRING_1));
		assertEquals(STRING_2, task.getString().get(STRING_2));
		assertNotEquals((Long) 0L, task.getDuration());
		assertNotEquals(task.getEndTime(), task.getStartTime());
	}

	@Test
	public void testStartSuccessInfoDifferentIndex() throws InterruptedException {
		Map<String, Task> newTasks = new HashMap<>();
		Map<String, Task> oldTasks = new HashMap<>();
		String id = Event.generateTaskId(ROLLOVER_TEST);

		LogParams startLogParams = LogParams.create().context(CTX_1, CTX_1).metric(METRIC_1,1).text(TEXT_1, TEXT_1).string(STRING_1, STRING_1);
		Event startEvent = new StartEvent(id, ROLLOVER_TEST, startLogParams, ROLLOVER_TEST, null);
		Thread.sleep(10);
		LogParams successLogParams = LogParams.create().context(CTX_2, CTX_2).metric(METRIC_2,2).text(TEXT_2, TEXT_2).string(STRING_2, STRING_2);
		Event successEvent = new SuccessEvent(id, successLogParams);
		List<Event> oldEvents = Lists.newArrayList(startEvent, successEvent);
		Task oldTask = new Task(oldEvents, 1);
		oldTasks.put(id, oldTask);


		LogParams infoLogParams = LogParams.create().context(CTX_3, CTX_3).metric(METRIC_3,3).text(TEXT_3, TEXT_3).string(STRING_3, STRING_3);
		InfoEvent infoEvent = new InfoEvent(id, infoLogParams);
		List<Event> newEvents = Lists.newArrayList(infoEvent);
		Task newTask = new Task(newEvents, 1);
		newTasks.put(id, newTask);


		TimberLogTest.client.index(oldTasks, TimberLogTest.client.getOldIndex());
		TimberLogTest.waitForTask(id, TaskStatus.SUCCESS);
		TimberLogTest.client.index(newTasks, TimberLogTest.client.getCurrentIndex());
		TimberLogTest.waitForTasks(id, 2);
		tasksMergerJobs.execute(context);

		TimberLogTest.waitForTask(id, TaskStatus.SUCCESS);
		Task task = TimberLogTest.client.getTaskById(id);

		assertNotNull(task);
		assertEquals(TaskStatus.SUCCESS, task.getStatus());
		assertEquals(CTX_1, task.getCtx().get(CTX_1));
		assertEquals(CTX_2, task.getCtx().get(CTX_2));
		assertEquals(CTX_3, task.getCtx().get(CTX_3));
		assertEquals(1, task.getMetric().get(METRIC_1).intValue());
		assertEquals(2, task.getMetric().get(METRIC_2).intValue());
		assertEquals(3, task.getMetric().get(METRIC_3).intValue());
		assertEquals(TEXT_1, task.getText().get(TEXT_1));
		assertEquals(TEXT_2, task.getText().get(TEXT_2));
		assertEquals(TEXT_3, task.getText().get(TEXT_3));
		assertEquals(STRING_1, task.getString().get(STRING_1));
		assertEquals(STRING_2, task.getString().get(STRING_2));
		assertEquals(STRING_3, task.getString().get(STRING_3));
		assertNotEquals((Long) 0L, task.getDuration());
		assertNotEquals(task.getEndTime(), task.getStartTime());
	}

	@Test
	public void testStartErrorInfoDifferentIndex() throws InterruptedException {
		Map<String, Task> newTasks = new HashMap<>();
		Map<String, Task> oldTasks = new HashMap<>();
		String id = Event.generateTaskId(ROLLOVER_TEST);

		LogParams startLogParams = LogParams.create().context(CTX_1, CTX_1).metric(METRIC_1,1).text(TEXT_1, TEXT_1).string(STRING_1, STRING_1);
		Event startEvent = new StartEvent(id, ROLLOVER_TEST, startLogParams, ROLLOVER_TEST, null);
		Thread.sleep(10);
		LogParams ErrorLogParams = LogParams.create().context(CTX_2, CTX_2).metric(METRIC_2,2).text(TEXT_2, TEXT_2).string(STRING_2, STRING_2);
		Event errorEvent = new ErrorEvent(id, ErrorLogParams);
		List<Event> oldEvents = Lists.newArrayList(startEvent, errorEvent);
		Task oldTask = new Task(oldEvents, 1);
		oldTasks.put(id, oldTask);


		LogParams infoLogParams = LogParams.create().context(CTX_3, CTX_3).metric(METRIC_3,3).text(TEXT_3, TEXT_3).string(STRING_3, STRING_3);
		InfoEvent infoEvent = new InfoEvent(id, infoLogParams);
		List<Event> newEvents = Lists.newArrayList(infoEvent);
		Task newTask = new Task(newEvents, 1);
		newTasks.put(id, newTask);


		TimberLogTest.client.index(oldTasks, TimberLogTest.client.getOldIndex());
		TimberLogTest.waitForTask(id, TaskStatus.ERROR);
		TimberLogTest.client.index(newTasks, TimberLogTest.client.getCurrentIndex());
		TimberLogTest.waitForTasks(id, 2);
		tasksMergerJobs.execute(context);

		TimberLogTest.waitForTask(id, TaskStatus.ERROR);
		Task task = TimberLogTest.client.getTaskById(id);

		assertNotNull(task);
		assertEquals(TaskStatus.ERROR, task.getStatus());
		assertEquals(CTX_1, task.getCtx().get(CTX_1));
		assertEquals(CTX_2, task.getCtx().get(CTX_2));
		assertEquals(CTX_3, task.getCtx().get(CTX_3));
		assertEquals(1, task.getMetric().get(METRIC_1).intValue());
		assertEquals(2, task.getMetric().get(METRIC_2).intValue());
		assertEquals(3, task.getMetric().get(METRIC_3).intValue());
		assertEquals(TEXT_1, task.getText().get(TEXT_1));
		assertEquals(TEXT_2, task.getText().get(TEXT_2));
		assertEquals(TEXT_3, task.getText().get(TEXT_3));
		assertEquals(STRING_1, task.getString().get(STRING_1));
		assertEquals(STRING_2, task.getString().get(STRING_2));
		assertEquals(STRING_3, task.getString().get(STRING_3));
		assertNotEquals((Long) 0L, task.getDuration());
		assertNotEquals(task.getEndTime(), task.getStartTime());
	}
}
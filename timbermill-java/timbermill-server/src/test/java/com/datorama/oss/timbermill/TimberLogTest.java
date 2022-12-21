package com.datorama.oss.timbermill;

import com.datorama.oss.timbermill.annotation.TimberLogTask;
import com.datorama.oss.timbermill.pipe.EventOutputPipe;
import com.datorama.oss.timbermill.unit.Event;
import com.datorama.oss.timbermill.unit.LogParams;
import com.datorama.oss.timbermill.unit.Task;
import com.datorama.oss.timbermill.unit.TaskStatus;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.awaitility.Awaitility;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static com.datorama.oss.timbermill.common.Constants.*;
import static org.junit.Assert.*;

public abstract class TimberLogTest {

	static final String TEST = "test";
	static final String EVENT = "Event";
	private static final Predicate<Task> notOrphanPredicate = (Task task) -> (task != null) && (task.isOrphan() == null || !task.isOrphan());
	private static final String EVENT_CHILD = "EventChild";
	private static final String EVENT_CHILD_OF_CHILD = "EventChildOfChild";
	private static final Exception FAIL = new Exception("fail");
	private static final String SPOT = "Spot";
	protected static ElasticsearchClientForTests client;
	private String childTaskId;
	private String childOfChildTaskId;

	protected synchronized static void waitForTask(String taskId, TaskStatus status) {
		waitForTask(taskId, status, client);
	}

	synchronized static void waitForTask(String taskId, TaskStatus status, ElasticsearchClient esClient) {
		Callable<Boolean> callable = () -> (esClient.getTaskById(taskId) != null) && (esClient.getTaskById(taskId).getStatus() == status);
		waitForCallable(callable);
	}

	static void waitForNonOrphanTask(String taskId) {
		Callable<Boolean> callable = () -> client.getTaskById(taskId) != null && TimberLogTest.notOrphanPredicate.test(client.getTaskById(taskId));
		waitForCallable(callable);
	}

	public static void waitForTasks(String taskId, int tasksAmounts) {
		Callable<Boolean> callable = () -> (client.getMultipleTasksById(taskId) != null) && (client.getMultipleTasksById(taskId).size() == tasksAmounts);
		waitForCallable(callable);
	}

	static void waitForCallable(Callable<Boolean> callable) {
		Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS).until(callable);
	}


	static void assertNotOrphan(Task task){
		assertTrue(task.isOrphan() == null || !task.isOrphan());
	}

	static void assertOrphan(Task task){
		assertFalse(task.isOrphan() == null || !task.isOrphan());
	}

	protected static void init(EventOutputPipe pipe) {
		String elasticUrl = System.getenv("ELASTICSEARCH_URL");
		if (StringUtils.isEmpty(elasticUrl)){
			elasticUrl = "http://localhost:9200";
		}

		client = new ElasticsearchClientForTests(elasticUrl, null);

		TimberLogger.bootstrap(pipe, TEST);

	}

	protected static void tearDown() {
		TimberLogger.exit();
	}

	static void assertTask(Task task, String name, boolean shouldHaveEndTime, boolean shouldBeComplete, String primaryId, String parentId, TaskStatus status, String... parents) {
		assertEquals(name, task.getName());
		assertEquals(status, task.getStatus());
		assertNotNull(task.getStartTime());
		if (shouldHaveEndTime){
			assertNotNull(task.getEndTime());
		}
		else{
			assertNull(task.getEndTime());
		}

		if (shouldBeComplete) {
			assertNotNull(task.getDuration());
		} else {
			assertNull(task.getDuration());
		}
		assertEquals(primaryId, task.getPrimaryId());
		assertEquals(parentId, task.getParentId());

		List<String> parentsPath = task.getParentsPath();
		if (parentId == null || (parents.length == 0)){
			assertNull(parentsPath);
		}
		else {
			assertNotNull(parentsPath);
			assertEquals(parents.length, parentsPath.size());
			for (String parent : parents) {
				assertTrue(parentsPath.contains(parent));
			}
		}
	}

	static void assertTaskPrimary(Task task, String name, TaskStatus status, String primaryId, boolean shouldHaveEndTime, boolean shouldBeComplete) {
		assertTask(task, name, shouldHaveEndTime, shouldBeComplete, primaryId, null, status);
	}

	static void assertTaskCorrupted(Task task, String name, TaskStatus status, boolean shouldHaveEndTime) {
		assertTask(task, name, shouldHaveEndTime, false, null, null, status);
	}

	protected void testSimpleTaskIndexerJob() throws InterruptedException {
		String str1 = "str1";
		String str2 = "str2";
		String metric1 = "metric1";
		String metric2 = "metric2";
		String text1 = "text1";
		String text2 = "text2";

		String hugeField = "hugeField";
		String taskId = testSimpleTaskIndexerJobTimberLog(str1, str2, metric1, metric2, text1, text2, hugeField);

		waitForTask(taskId, TaskStatus.SUCCESS, client);
		Task task = client.getTaskById(taskId);

		assertTaskPrimary(task, EVENT, TaskStatus.SUCCESS, taskId, true, true);
		assertEquals(TEST, task.getEnv());
		assertNotNull(task.getDateToDelete());
		assertTrue(task.getDuration() >= 1000);
		Map<String, String> strings = task.getString();
		Map<String, String> ctx = task.getCtx();
		Map<String, Number> metrics = task.getMetric();
		Map<String, String> texts = task.getText();

		assertEquals(str1, strings.get(str1));
		assertEquals(str2, strings.get(str2));
		assertEquals(0, metrics.get(metric1).intValue());
		assertEquals(2, metrics.get(metric2).intValue());
		assertEquals(text1, texts.get(text1));
		assertEquals(text2, texts.get(text2));
		assertEquals(MAX_CHARS_ALLOWED_FOR_ANALYZED_FIELDS, texts.get(hugeField).length());
		assertEquals(MAX_CHARS_ALLOWED_FOR_NON_ANALYZED_FIELDS, strings.get(hugeField).length());
		assertEquals(MAX_CHARS_ALLOWED_FOR_NON_ANALYZED_FIELDS, ctx.get(hugeField).length());
	}

	protected void testSimpleTaskWithParams(){
		String str1 = "str1";
		String str2 = "str2";

		String taskId = simpleTaskWithParams(str1, str2);

		waitForTask(taskId, TaskStatus.SUCCESS, client);
		Task task = client.getTaskById(taskId);

		assertTaskPrimary(task, EVENT, TaskStatus.SUCCESS, taskId, true, true);
		Map<String, String> strings = task.getString();

		assertEquals(str1, strings.get("str"));
		assertEquals(str2, strings.get(str2));
	}

	protected void testEndWithingAnnotation(){
		List<String> taskIds = simpleTask1();

		String taskId3 = taskIds.get(0);
		String taskId2 = taskIds.get(1);
		String taskId1 = taskIds.get(2);

		waitForTask(taskId1, TaskStatus.SUCCESS, client);
		waitForTask(taskId2, TaskStatus.ERROR, client);
		waitForTask(taskId3, TaskStatus.SUCCESS, client);

	}

	@TimberLogTask(name = EVENT  + "1")
	private ArrayList<String> simpleTask1() {
		String currentTaskId = TimberLogger.getCurrentTaskId();
		ArrayList<String> ids = simpleTask2();
		TimberLogger.success();
		ids.add(currentTaskId);
		return ids;
	}

	@TimberLogTask(name = EVENT + "2")
	private ArrayList<String> simpleTask2() {
		String currentTaskId = TimberLogger.getCurrentTaskId();
		String taskId3 = simpleTask3();
		TimberLogger.error();
		return Lists.newArrayList(taskId3, currentTaskId);
	}

	@TimberLogTask(name = EVENT + "3")
	private String simpleTask3() {
		String currentTaskId = TimberLogger.getCurrentTaskId();
		TimberLogger.success();
		return currentTaskId;
	}

	@TimberLogTask(name = EVENT, logParameters = true)
	private String simpleTaskWithParams(String str, String str2) {
		return TimberLogger.getCurrentTaskId();
	}

	@TimberLogTask(name = EVENT)
	private String testSimpleTaskIndexerJobTimberLog(String str1, String str2, String metric1, String metric2, String text1, String text2, String hugeField) throws InterruptedException {
		TimberLogger.logString(str1, str1);
		TimberLogger.logMetric(metric1, Double.NaN);
		TimberLogger.logText(text1, text1);
		StringBuilder stringBuilder = new StringBuilder();
		for (int i = 0; i < 3270660; i++) {
			stringBuilder.append("a");
		}
		String hugeString = stringBuilder.toString();
		TimberLogger.logText(hugeField, hugeString);
		TimberLogger.logString(hugeField, hugeString);
		TimberLogger.logContext(hugeField, hugeString);
		TimberLogger.logContext(null, "null");
		TimberLogger.logParams(LogParams.create().string(str2, str2).metric(metric2, 2).text(text2, text2));
		Thread.sleep(1000);
		return TimberLogger.getCurrentTaskId();
	}

	protected void testSwitchCasePlugin() {

		String taskId = testSwitchCasePluginLog();

		waitForTask(taskId, TaskStatus.SUCCESS, client);
		Task task = client.getTaskById(taskId);

		assertTaskPrimary(task, EVENT + "plugin", TaskStatus.SUCCESS, taskId, true, true);
		Map<String, String> strings = task.getString();
		String errorType = strings.get("errorType");
		assertEquals("TOO_MANY_SERVER_ROWS", errorType);
	}

	@TimberLogTask(name = EVENT + "plugin")
	private String testSwitchCasePluginLog() {
		TimberLogger.logText("exception", "bla bla bla TOO_MANY_SERVER_ROWS bla bla bla");
		return TimberLogger.getCurrentTaskId();
	}

	protected void testSpotWithParent() {
		String context = "context";
		String str = "str";

		final String[] taskIdSpot = {null};
		Pair<String, String> stringStringPair = testSpotWithParentTimberLog(context, str, taskIdSpot);

		String taskId1 = stringStringPair.getLeft();
		String taskId2 = stringStringPair.getRight();
		waitForTask(taskId1, TaskStatus.SUCCESS, client);
		waitForTask(taskId2, TaskStatus.SUCCESS, client);
		waitForTask(taskIdSpot[0], TaskStatus.SUCCESS, client);

		Task task = client.getTaskById(taskId1);
		assertTaskPrimary(task, EVENT, TaskStatus.SUCCESS, taskId1, true, true);

		Task spot = client.getTaskById(taskIdSpot[0]);
		assertTask(spot, SPOT, true, true, taskId1, taskId1, TaskStatus.SUCCESS, EVENT);
		assertEquals(context, spot.getCtx().get(context));

		Task child = client.getTaskById(taskId2);
		assertTask(child, EVENT + '2', true, true, taskId1, taskId1, TaskStatus.SUCCESS, EVENT);
		assertEquals(context, child.getCtx().get(context));
	}

	@TimberLogTask(name = EVENT)
	private Pair<String, String> testSpotWithParentTimberLog(String context1, String context2, String[] taskIdSpot) {
		String taskId1 = TimberLogger.getCurrentTaskId();
		TimberLogger.logContext(context1, context1);
		String taskId2 = testSpotWithParentTimberLog2(context2, taskIdSpot, taskId1);
		return Pair.of(taskId1, taskId2);
	}

	@TimberLogTask(name = EVENT + '2')
	private String testSpotWithParentTimberLog2(String context2, String[] taskIdSpot, String taskId1) {
		TimberLogger.logContext(context2, context2);
		Thread thread = new Thread(() -> {
			try (TimberLogContext ignored = new TimberLogContext(taskId1)) {
				taskIdSpot[0] = TimberLogger.spot(SPOT);
			}
		});
		thread.start();
		while(thread.isAlive()){
			waits();
		}
		return TimberLogger.getCurrentTaskId();
	}

	private void waits() {
		try {
			Thread.sleep(1000);
		} catch (InterruptedException ignored) {
		}
	}

	protected void testSimpleTasksFromDifferentThreadsIndexerJob() {

		String context1 = "context1";
		String context2 = "context2";

		String[] tasks = testSimpleTasksFromDifferentThreadsIndexerJobTimberLog1(context1, context2);
		String taskId = tasks[0];
		String taskId2 = tasks[1];
		String taskId3 = tasks[2];
		String taskIdSpot = tasks[3];


		waitForTask(taskId, TaskStatus.SUCCESS, client);
		waitForTask(taskId2, TaskStatus.SUCCESS, client);
		waitForTask(taskId3, TaskStatus.SUCCESS, client);
		waitForTask(taskIdSpot, TaskStatus.SUCCESS, client);


		Task task = client.getTaskById(taskId);
		assertTaskPrimary(task, EVENT, TaskStatus.SUCCESS, taskId, true, true);
		assertEquals(context1, task.getCtx().get(context1));

		Task task2 = client.getTaskById(taskId2);
		assertTask(task2, EVENT + '2', true, true, taskId, taskId, TaskStatus.SUCCESS, EVENT);
		assertEquals(context1, task2.getCtx().get(context1));
		assertEquals(context2, task2.getCtx().get(context2));

		Task task3 = client.getTaskById(taskId3);
		assertTask(task3, EVENT + '3', true, true, taskId, taskId2, TaskStatus.SUCCESS, EVENT, EVENT + '2');
		assertEquals(context1, task3.getCtx().get(context1));
		assertEquals(context2, task3.getCtx().get(context2));

		Task spot = client.getTaskById(taskIdSpot);
		assertTask(spot, SPOT, true, true, taskId, taskId, TaskStatus.SUCCESS, EVENT);
		assertEquals(context1, spot.getCtx().get(context1));
	}

	@TimberLogTask(name = EVENT)
	private String[] testSimpleTasksFromDifferentThreadsIndexerJobTimberLog1(String context1, String context2) {
		TimberLogger.logContext(context1, context1);
		String taskId = TimberLogger.getCurrentTaskId();
		String[] tasks = testSimpleTasksFromDifferentThreadsIndexerJobTimberLog2(context2, taskId);
		tasks[0] = TimberLogger.getCurrentTaskId();
		return tasks;
	}

	@TimberLogTask(name = EVENT + '2')
	private String[] testSimpleTasksFromDifferentThreadsIndexerJobTimberLog2(String context2, String taskId) {

		TimberLogger.logContext(context2, context2);

		final String[] tasks = new String[4];
		tasks[1] = TimberLogger.getCurrentTaskId();
		Thread thread1 = new Thread(() -> {
			try (TimberLogContext ignored = new TimberLogContext(tasks[1])) {
				tasks[2] = testSimpleTasksFromDifferentThreadsIndexerJobTimberLog3();
			}
		});
		thread1.start();

		Thread thread2 = new Thread(() -> {
			try (TimberLogContext ignored = new TimberLogContext(taskId)) {
				tasks[3] = testSimpleTasksFromDifferentThreadsIndexerJobTimberLog4();
			}
		});
		thread2.start();
		while(thread1.isAlive()){waits();}
		while(thread2.isAlive()){waits();}
		return tasks;
	}

	@TimberLogTask(name = EVENT + '3')
	private String testSimpleTasksFromDifferentThreadsIndexerJobTimberLog3() {
		return TimberLogger.getCurrentTaskId();
	}

	@TimberLogTask(name = SPOT)
	private String testSimpleTasksFromDifferentThreadsIndexerJobTimberLog4() {
		return TimberLogger.getCurrentTaskId();
	}

	void testMissingParentTaskFromDifferentThreads(boolean shouldRollover) {
		String spotId = Event.generateTaskId(SPOT);

		String context1 = "context1";
		final String[] tasks = new String[2];
		Thread thread = new Thread(() -> {
			tasks[0] = TimberLogger.start(EVENT, spotId, LogParams.create().context(context1, context1));
			tasks[1] = TimberLogger.start(EVENT_CHILD);
			TimberLogger.success();
			TimberLogger.success();
		});
		thread.start();
		while(thread.isAlive()){waits();}

		String taskId1 = tasks[0];
		String taskId2 = tasks[1];

		waitForTask(taskId1, TaskStatus.SUCCESS, client);
		waitForTask(taskId2, TaskStatus.SUCCESS, client);

		if (shouldRollover) {
			client.rolloverIndexForTest(TEST);
		}

		String context2 = "context2";

		TimberLogger.spot(spotId, SPOT, null, LogParams.create().context(context2, context2));

		waitForTask(spotId, TaskStatus.SUCCESS, client);

		waitForNonOrphanTask(taskId1);
		waitForNonOrphanTask(taskId2);

		Task task1 = client.getTaskById(taskId1);
		assertTask(task1, EVENT, true, true, spotId, spotId, TaskStatus.SUCCESS, SPOT);
		assertEquals(context1, task1.getCtx().get(context1));
		assertEquals(context2, task1.getCtx().get(context2));

		Task task2 = client.getTaskById(taskId2);
		assertTask(task2, EVENT_CHILD, true, true, spotId, taskId1, TaskStatus.SUCCESS, SPOT, EVENT);
		assertEquals(context1, task2.getCtx().get(context1));
		assertEquals(context2, task2.getCtx().get(context2));
	}

	void testMissingParentTaskOutOffOrderFromDifferentThreads(boolean shouldRollover) {
		String spotId = Event.generateTaskId(SPOT);

		String context1 = "context1";
		final String[] tasks = new String[2];
		Thread thread = new Thread(() -> {
			String eventId = Event.generateTaskId(EVENT);
			tasks[1] = TimberLogger.start(EVENT_CHILD, eventId, LogParams.create());
			TimberLogger.success();
			tasks[0] = TimberLogger.start(eventId, EVENT, spotId, LogParams.create().context(context1, context1));
			TimberLogger.success();
		});
		thread.start();
		while(thread.isAlive()){waits();}

		String taskId1 = tasks[0];
		String taskId2 = tasks[1];

		waitForTask(taskId1, TaskStatus.SUCCESS, client);
		waitForTask(taskId2, TaskStatus.SUCCESS, client);

		if (shouldRollover) {
			client.rolloverIndexForTest(TEST);
		}

		String context2 = "context2";

		TimberLogger.spot(spotId, SPOT, null, LogParams.create().context(context2, context2));

		waitForTask(spotId, TaskStatus.SUCCESS, client);
		waitForNonOrphanTask(taskId1);
		waitForNonOrphanTask(taskId2);

		Task task1 = client.getTaskById(taskId1);
		assertTask(task1, EVENT, true, true, spotId, spotId, TaskStatus.SUCCESS, SPOT);
		assertEquals(context1, task1.getCtx().get(context1));
		assertEquals(context2, task1.getCtx().get(context2));

		Task task2 = client.getTaskById(taskId2);
		assertTask(task2, EVENT_CHILD, true, true, spotId, taskId1, TaskStatus.SUCCESS, SPOT, EVENT);
		assertEquals(context1, task2.getCtx().get(context1));
		assertEquals(context2, task2.getCtx().get(context2));
	}

	protected void testSimpleTasksFromDifferentThreadsWithWrongParentIdIndexerJob() {
		final String[] taskId = new String[1];
		Thread thread = new Thread(() -> {
			try (TimberLogContext ignored = new TimberLogContext("bla")) {
				taskId[0] = testSimpleTasksFromDifferentThreadsWithWrongParentIdIndexerJobTimberLog();
			}

		});
		thread.start();
		while(thread.isAlive()){waits();}

		waitForTask(taskId[0], TaskStatus.SUCCESS, client);

		Task task = client.getTaskById(taskId[0]);
		assertTask(task, EVENT, true, true, null, "bla", TaskStatus.SUCCESS);
	}

	@TimberLogTask(name = EVENT)
	private String testSimpleTasksFromDifferentThreadsWithWrongParentIdIndexerJobTimberLog() {
		return TimberLogger.getCurrentTaskId();
	}

	protected void testComplexTaskIndexerWithErrorTask() {

		String parentId = testComplexTaskIndexerWithErrorTaskTimberLog3();
		waitForTask(parentId, TaskStatus.SUCCESS, client);
		waitForTask(childTaskId, TaskStatus.ERROR, client);
		waitForTask(childOfChildTaskId, TaskStatus.ERROR, client);

		Task taskParent = client.getTaskById(parentId);
		Task taskChild = client.getTaskById(childTaskId);
		Task taskChildOfChild = client.getTaskById(childOfChildTaskId);

		assertTaskPrimary(taskParent, EVENT, TaskStatus.SUCCESS, parentId, true, true);

		assertTask(taskChild, EVENT_CHILD, true, true, parentId, parentId, TaskStatus.ERROR, EVENT);

		assertTask(taskChildOfChild, EVENT_CHILD_OF_CHILD, true, true, parentId, childTaskId, TaskStatus.ERROR, EVENT, EVENT_CHILD);
	}

	@TimberLogTask(name = EVENT)
	private String testComplexTaskIndexerWithErrorTaskTimberLog3(){
		try {
			testComplexTaskIndexerWithErrorTaskTimberLog2();
		} catch (Exception ignored) {
		}
		return TimberLogger.getCurrentTaskId();
	}

	@TimberLogTask(name = EVENT_CHILD)
	private void testComplexTaskIndexerWithErrorTaskTimberLog2() throws Exception {
		childTaskId = TimberLogger.getCurrentTaskId();
		testComplexTaskIndexerWithErrorTaskTimberLog1();
	}

	@TimberLogTask(name = EVENT_CHILD_OF_CHILD)
	private void testComplexTaskIndexerWithErrorTaskTimberLog1() throws Exception {
		childOfChildTaskId = TimberLogger.getCurrentTaskId();
		throw FAIL;
	}

	protected void testTaskWithNullString() {

		String taskId = testTaskWithNullStringTimberLog();
		waitForTask(taskId, TaskStatus.SUCCESS, client);

		Task task = client.getTaskById(taskId);
		assertTaskPrimary(task, EVENT, TaskStatus.SUCCESS, taskId, true, true);
	}

	@TimberLogTask(name = EVENT)
	private String testTaskWithNullStringTimberLog() {
		TimberLogger.logString("key", "null");
		return TimberLogger.getCurrentTaskId();
	}

	protected void testOverConstructor() {

		TimberLogTestClass timberLogTestClass = new TimberLogTestClass();
		String taskId = timberLogTestClass.getTaskId();
		waitForTask(taskId, TaskStatus.SUCCESS, client);

		Task task = client.getTaskById(taskId);
		assertTaskPrimary(task, "ctr", TaskStatus.SUCCESS, taskId, true, true);
	}

	protected void testOverConstructorException() {
		String[] taskArr = new String[1];
		try {
			new TimberLogTestClass(taskArr);
		} catch (Exception ignored) {
		}
		String taskId = taskArr[0];
		waitForTask(taskId, TaskStatus.ERROR, client);

		Task task = client.getTaskById(taskId);
		assertTaskPrimary(task, "ctr", TaskStatus.ERROR, taskId, true, true);
	}

	protected void testCorruptedInfoOnly() {
		String bla1 = "bla1";
		String bla2 = "bla2";
		String taskId = TimberLogger.logText(bla1, bla2);
		waitForTask(taskId, TaskStatus.CORRUPTED, client);

		Task task = client.getTaskById(taskId);
		assertEquals(LOG_WITHOUT_CONTEXT, task.getName());
		Map<String, String> text = task.getText();
		assertNotNull(text.get(EventLogger.STACK_TRACE));
		assertEquals(bla2, text.get(bla1));
	}

	protected void testOrphan() {
		String taskId = testOrphanTimberLog();
		waitForTask(taskId, TaskStatus.SUCCESS, client);
		Task task = client.getTaskById(taskId);
		assertTrue(task.isOrphan());
	}

	private String testOrphanTimberLog() {
		String taskId = TimberLogger.start("testOrphan", "testOrphanParent");
		TimberLogger.success();
		return taskId;
	}

	public void testTaskReachedRateLimit() {
		String ongoingTaskName = EVENT + "_rate_limit";
		String limitStr = System.getenv("LIMIT_FOR_PERIOD");
		int numOfEvents = limitStr != null ? Integer.valueOf(limitStr) + 1 : 51;
		String[] taskIds = new String[numOfEvents];

		for (int i = 0; i < numOfEvents; i++) {
			taskIds[i] = TimberLogger.spot(ongoingTaskName);
		}

		TimberLogTest.waitForTask(taskIds[numOfEvents - 2], TaskStatus.SUCCESS);

		for (int i = 0; i < numOfEvents - 1; i++) {
			Task ongoingTask = client.getTaskById(taskIds[i]);
			assertTaskPrimary(ongoingTask, ongoingTaskName, TaskStatus.SUCCESS, taskIds[i], true, true);
		}

		assertNull(client.getTaskById(taskIds[numOfEvents - 1]));

		try {
			Thread.sleep(60000);
		} catch (InterruptedException ignored) {
		}

		String taskId = TimberLogger.spot(ongoingTaskName);

		TimberLogTest.waitForTask(taskId, TaskStatus.SUCCESS);

		Task ongoingTask = client.getTaskById(taskId);
		assertTaskPrimary(ongoingTask, ongoingTaskName, TaskStatus.SUCCESS, taskId, true, true);
	}
}
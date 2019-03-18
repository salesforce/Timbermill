package com.datorama.timbermill;

import com.datorama.timbermill.annotation.TimberLog;
import com.datorama.timbermill.pipe.LocalOutputPipeConfig;
import com.datorama.timbermill.unit.Task;
import org.apache.commons.lang3.tuple.Pair;
import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.datorama.timbermill.unit.Task.TaskStatus;
import static org.junit.Assert.*;

public class TimberLogTest {

	private static final String EVENT = "Event";
	private static final String EVENT_CHILD = "EventChild";
	private static final String EVENT_CHILD_OF_CHILD = "EventChildOfChild";
	private static final Exception FAIL = new Exception("fail");
	private static final String SPOT = "Spot";
	private static final String TEST = "test";
	private static final String HTTP_LOCALHOST_9200 = "http://localhost:9200";
	private static ElasticsearchClient client = new ElasticsearchClient(TEST, HTTP_LOCALHOST_9200, 1000, 0);
	private String childTaskId;
	private String childOfChildTaskId;

	@BeforeClass
	public static void init() {
		Map<String, Integer> map = Collections.singletonMap("text.sql", 1000);
		LocalOutputPipeConfig.Builder builder = new LocalOutputPipeConfig.Builder().env(TEST).url(HTTP_LOCALHOST_9200).defaultMaxChars(10000).propertiesLengthMap(map).secondBetweenPolling(1);
		LocalOutputPipeConfig config = new LocalOutputPipeConfig(builder);
		TimberLogger.bootstrap(config);
	}

	@AfterClass
	public static void kill() {
		TimberLogger.exit();
	}

	@Test
	public void testSimpleTaskIndexerJob() {
		String str1 = "str1";
		String str2 = "str2";
		String str3 = "str3";
		String metric1 = "metric1";
		String metric2 = "metric2";
		String text1 = "text1";
		String text2 = "text2";

		String taskId = testSimpleTaskIndexerJobTimberLog(str1, str2, str3, metric1, metric2, text1, text2);

		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null)
				&& (client.getTaskById(taskId).getStatus() == TaskStatus.SUCCESS));
		Task task = client.getTaskById(taskId);
		assertEquals(EVENT, task.getName());
		assertEquals(taskId, task.getPrimaryId());
		assertEquals(TaskStatus.SUCCESS, task.getStatus());
		assertEquals(TEST, task.getEnv());

		Map<String, Object> strings = task.getString();
		Map<String, Number> metrics = task.getMetric();
		Map<String, String> texts = task.getText();

		assertEquals(str2, strings.get(str2));
		assertEquals(str3, strings.get(str3));
		assertEquals(str1, strings.get(str1));
		assertEquals(1, metrics.get(metric1).intValue());
		assertEquals(2, metrics.get(metric2).intValue());
		assertEquals(text1, texts.get(text1));
		assertEquals(text2, texts.get(text2));

		assertNull(task.getParentId());
	}

	@TimberLog(name = EVENT)
	private String testSimpleTaskIndexerJobTimberLog(String str1, String str2, String str3, String metric1, String metric2, String text1, String text2) {
		TimberLogger.logString(str1, str1);
		TimberLogger.logMetric(metric1, 1);
		TimberLogger.logText(text1, text1);
		TimberLogger.logString(str2, str2);
		TimberLogger.logParams(LogParams.create().string(str3, str3).metric(metric2, 2).text(text2, text2));
		return TimberLogger.getCurrentTaskId();
	}

	@Test
	public void testSimpleTaskWithTrimmer() {
		String str = "string";
		String text = "sql";

		StringBuilder sb = new StringBuilder();

		for (int i = 0 ; i < 1000000; i++){
			sb.append("a");
		}
		String hugeString = sb.toString();

		String taskId = testSimpleTaskWithTrimmer1(str, text, hugeString);
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null)
				&& (client.getTaskById(taskId).getStatus() == TaskStatus.SUCCESS));

		Task task = client.getTaskById(taskId);
		assertEquals(EVENT, task.getName());
		Map<String, Object> strings = task.getString();
		Map<String, String> texts = task.getText();
		assertEquals(10000, String.valueOf(strings.get(str)).length());
		assertEquals(1000, String.valueOf(texts.get(text)).length());
	}

	@TimberLog(name = EVENT)
	private String testSimpleTaskWithTrimmer1(String str, String text, String hugeString) {
		TimberLogger.logString(str, hugeString);
		TimberLogger.logText(text, hugeString);
		return TimberLogger.getCurrentTaskId();
	}

	@Test
	public void testSpotWithParent(){
		String global1 = "global1";
		String str2 = "str2";

		final String[] taskIdSpot = {null};
		Pair<String, String> stringStringPair = testSpotWithParentTimberLog(global1, str2, taskIdSpot);

		String taskId1 = stringStringPair.getLeft();
		String taskId2 = stringStringPair.getRight();
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId1) != null)
				&& (client.getTaskById(taskId1).getStatus() == TaskStatus.SUCCESS));
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId1) != null)
				&& (client.getTaskById(taskId1).getStatus() == TaskStatus.SUCCESS));
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskIdSpot[0]) != null)
				&& (client.getTaskById(taskId2).getStatus() == TaskStatus.SUCCESS));

		Task task = client.getTaskById(taskId1);
		assertEquals(EVENT, task.getName());
		assertEquals(taskId1, task.getPrimaryId());
		assertEquals(TaskStatus.SUCCESS, task.getStatus());
		assertNull(task.getParentId());

		Task spot = client.getTaskById(taskIdSpot[0]);
		assertEquals(SPOT, spot.getName());
		assertEquals(global1, spot.getGlobal().get(global1));
		assertEquals(taskId1, spot.getPrimaryId());
		assertEquals(TaskStatus.SUCCESS, spot.getStatus());
		assertEquals(taskId1, spot.getParentId());
		assertTrue(spot.getParentsPath().contains(EVENT));
	}

	@TimberLog(name = EVENT)
	private Pair<String, String> testSpotWithParentTimberLog(String global1, String global2, String[] taskIdSpot) {
		String taskId1 = TimberLogger.getCurrentTaskId();
		TimberLogger.logGlobal(global1, global1);
		String taskId2 = testSpotWithParentTimberLog2(global2, taskIdSpot, taskId1);
		return Pair.of(taskId1, taskId2);
	}


	@TimberLog(name = EVENT + '2')
	private String testSpotWithParentTimberLog2(String global2, String[] taskIdSpot, String taskId1) {
		TimberLogger.logGlobal(global2, global2);
		new Thread(() -> {
			try(TimberLogContext ignored = new TimberLogContext(taskId1)){
				taskIdSpot[0] = TimberLogger.spot(SPOT);
			}
		}).run();
		return TimberLogger.getCurrentTaskId();
	}

	@Test
	public void testSimpleTasksFromDifferentThreadsIndexerJob(){

		String global1 = "global1";
		String global2 = "global2";

		String[] tasks = testSimpleTasksFromDifferentThreadsIndexerJobTimberLog1(global1, global2);
		String taskId = tasks[0];
		String taskId2 = tasks[1];
		String taskId3 = tasks[2];
		String taskIdSpot = tasks[3];


		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null)
				&& (client.getTaskById(taskId).getStatus() == TaskStatus.SUCCESS));
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId2) != null)
				&& (client.getTaskById(taskId2).getStatus() == TaskStatus.SUCCESS));
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId3) != null)
				&& (client.getTaskById(taskId3).getStatus() == TaskStatus.SUCCESS));
		Task task = client.getTaskById(taskId);
		assertEquals(EVENT, task.getName());
		assertEquals(taskId, task.getPrimaryId());
		assertEquals(TaskStatus.SUCCESS, task.getStatus());
		assertNull(task.getParentId());

		Task task3 = client.getTaskById(taskId3);
		assertEquals(EVENT + '3', task3.getName());
		assertEquals(global1, task3.getGlobal().get(global1));
		assertEquals(global2, task3.getGlobal().get(global2));
		assertEquals(taskId, task3.getPrimaryId());
		assertEquals(TaskStatus.SUCCESS, task3.getStatus());
		assertEquals(taskId2, task3.getParentId());
		assertTrue(task3.getParentsPath().contains(EVENT));
		assertTrue(task3.getParentsPath().contains(EVENT + '2'));

		Task spot = client.getTaskById(taskIdSpot);
		assertEquals(SPOT, spot.getName());
		assertEquals(global1, spot.getGlobal().get(global1));
		assertEquals(taskId, spot.getPrimaryId());
		assertEquals(TaskStatus.SUCCESS, spot.getStatus());
		assertEquals(taskId, spot.getParentId());
		assertTrue(spot.getParentsPath().contains(EVENT));
	}

	@TimberLog(name = EVENT)
	private String[] testSimpleTasksFromDifferentThreadsIndexerJobTimberLog1(String global1, String global2) {
		TimberLogger.logGlobal(global1, global1);
		String taskId = TimberLogger.getCurrentTaskId();
		String[] tasks = testSimpleTasksFromDifferentThreadsIndexerJobTimberLog2(global2, taskId);
		tasks[0] = TimberLogger.getCurrentTaskId();
		return tasks;
	}

	@TimberLog(name = EVENT + '2')
	private String[] testSimpleTasksFromDifferentThreadsIndexerJobTimberLog2(String global2, String taskId) {

		TimberLogger.logGlobal(global2, global2);

		final String[] tasks = new String[4];
		tasks[1] = TimberLogger.getCurrentTaskId();
		new Thread(() -> {
			try(TimberLogContext ignored = new TimberLogContext(tasks[1])){
				tasks[2] = testSimpleTasksFromDifferentThreadsIndexerJobTimberLog3();
			}
		}).run();

		new Thread(() -> {
			try(TimberLogContext ignored = new TimberLogContext(taskId)) {
				tasks[3] = testSimpleTasksFromDifferentThreadsIndexerJobTimberLog4();
			}
		}).run();
		return tasks;
	}

	@TimberLog(name = EVENT + '3')
	private String testSimpleTasksFromDifferentThreadsIndexerJobTimberLog3() {
		return TimberLogger.getCurrentTaskId();
	}

	@TimberLog(name = SPOT)
	private String testSimpleTasksFromDifferentThreadsIndexerJobTimberLog4() {
		return TimberLogger.getCurrentTaskId();
	}

	@Test
	public void testSimpleTasksFromDifferentThreadsWithWrongParentIdIndexerJob() {
		final String[] taskId = new String[1];
		new Thread(() -> {
			try(TimberLogContext ignored = new TimberLogContext("bla")){
				taskId[0] = testSimpleTasksFromDifferentThreadsWithWrongParentIdIndexerJobTimberLog();
			}

		}).run();



		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId[0]) != null)
				&& (client.getTaskById(taskId[0]).getStatus() == TaskStatus.SUCCESS));

		Task task = client.getTaskById(taskId[0]);
		assertEquals(EVENT, task.getName());
		assertEquals(taskId[0], task.getPrimaryId());
		assertEquals(TaskStatus.SUCCESS, task.getStatus());
		assertNull(task.getParentId());
	}

	@TimberLog(name = EVENT)
	private String testSimpleTasksFromDifferentThreadsWithWrongParentIdIndexerJobTimberLog() {
		return TimberLogger.getCurrentTaskId();
	}

	@Test
	public void testComplexTaskIndexerWithErrorTask() {

		String parentId = testComplexTaskIndexerWithErrorTaskTimberLog3();
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(parentId) != null)
				&& (client.getTaskById(parentId).getStatus() == TaskStatus.SUCCESS));
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> client.getTaskById(childTaskId) != null);
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> client.getTaskById(childOfChildTaskId) != null);

		Task taskParent = client.getTaskById(parentId);
		Task taskChild = client.getTaskById(childTaskId);
		Task taskChildOfChild = client.getTaskById(childOfChildTaskId);

		assertSame(TaskStatus.SUCCESS, taskParent.getStatus());
		assertNull(taskParent.getParentsPath());

		assertSame(TaskStatus.ERROR, taskChild.getStatus());
		assertEquals(parentId, taskChild.getPrimaryId());
		assertEquals(parentId, taskChild.getParentId());
		assertEquals(1, taskChild.getParentsPath().size());
		assertTrue(taskChild.getParentsPath().contains(taskParent.getName()));

		assertSame(TaskStatus.ERROR, taskChildOfChild.getStatus());
		assertEquals(parentId, taskChildOfChild.getPrimaryId());
		assertEquals(childTaskId, taskChildOfChild.getParentId());
		assertEquals(2, taskChildOfChild.getParentsPath().size());
		assertEquals(taskChildOfChild.getParentsPath().get(0), taskParent.getName());
		assertEquals(taskChildOfChild.getParentsPath().get(1), taskChild.getName());
	}

	@TimberLog(name = EVENT)
	private String testComplexTaskIndexerWithErrorTaskTimberLog3(){
		try {
			testComplexTaskIndexerWithErrorTaskTimberLog2();
		} catch (Exception ignored) {
		}
		return TimberLogger.getCurrentTaskId();
	}

	@TimberLog(name = EVENT_CHILD)
	private void testComplexTaskIndexerWithErrorTaskTimberLog2() throws Exception {
		childTaskId = TimberLogger.getCurrentTaskId();
		testComplexTaskIndexerWithErrorTaskTimberLog1();
	}

	@TimberLog(name = EVENT_CHILD_OF_CHILD)
	private void testComplexTaskIndexerWithErrorTaskTimberLog1() throws Exception {
		childOfChildTaskId = TimberLogger.getCurrentTaskId();
		throw FAIL;
	}

	@Test
	public void testTaskWithNullString() {

		String taskId = testTaskWithNullStringTimberLog();
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null)
				&& (client.getTaskById(taskId).getStatus() == TaskStatus.SUCCESS));

		Task task = client.getTaskById(taskId);
		assertSame(TaskStatus.SUCCESS, task.getStatus());
	}

	@TimberLog(name = EVENT)
	private String testTaskWithNullStringTimberLog() {
		TimberLogger.logString("key", "null");
		return TimberLogger.getCurrentTaskId();
	}

	@Test
	public void testOverConstructor() {

		TimberLogTestClass timberLogTestClass = new TimberLogTestClass();
		String taskId = timberLogTestClass.getTaskId();
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null)
				&& (client.getTaskById(taskId).getStatus() == TaskStatus.SUCCESS));

		Task task = client.getTaskById(taskId);
		assertSame(TaskStatus.SUCCESS, task.getStatus());
	}

	@Test
	public void testOverConstructorException() {
		String[] taskArr = new String[1];
		try {
			new TimberLogTestClass(taskArr);
		} catch (Exception ignored) {
		}
		String taskId = taskArr[0];
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null)
				&& (client.getTaskById(taskId).getStatus() == TaskStatus.ERROR));

		Task task = client.getTaskById(taskId);
		assertSame(TaskStatus.ERROR, task.getStatus());
	}

}
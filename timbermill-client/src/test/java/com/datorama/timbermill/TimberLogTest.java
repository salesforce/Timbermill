package com.datorama.timbermill;

import com.datorama.timbermill.annotation.TimberLog;
import com.datorama.timbermill.pipe.LocalOutputPipeConfig;
import com.datorama.timbermill.unit.Task;
import com.jayway.awaitility.Awaitility;
import org.apache.commons.lang3.tuple.Pair;
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
		Map<String, Integer> map = Collections.singletonMap("data.sql", 1000);
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
		String attr1 = "attr1";
		String attr2 = "attr2";
		String attr3 = "attr3";
		String metric1 = "metric1";
		String metric2 = "metric2";
		String data1 = "data1";
		String data2 = "data2";

		String taskId = testSimpleTaskIndexerJobTimberLog(attr1, attr2, attr3, metric1, metric2, data1, data2);

		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null)
				&& (client.getTaskById(taskId).getStatus() == TaskStatus.SUCCESS));
		Task task = client.getTaskById(taskId);
		assertEquals(EVENT, task.getTaskType());
		assertEquals(taskId, task.getPrimaryTaskId());
		assertEquals(TaskStatus.SUCCESS, task.getStatus());
		assertEquals(TEST, task.getEnv());

		Map<String, Object> attributes = task.getAttributes();
		Map<String, Number> metrics = task.getMetrics();
		Map<String, String> data = task.getData();

		assertEquals(attr2, attributes.get(attr2));
		assertEquals(attr3, attributes.get(attr3));
		assertEquals(attr1, attributes.get(attr1));
		assertEquals(1, metrics.get(metric1).intValue());
		assertEquals(2, metrics.get(metric2).intValue());
		assertEquals(data1, data.get(data1));
		assertEquals(data2, data.get(data2));

		assertNull(task.getParentTaskId());
	}

	@com.datorama.timbermill.annotation.TimberLog(taskType = EVENT)
	private String testSimpleTaskIndexerJobTimberLog(String attr1, String attr2, String attr3, String metric1, String metric2, String data1, String data2) {
		TimberLogger.logAttributes(attr1, attr1);
		TimberLogger.logMetrics(metric1, 1);
		TimberLogger.logData(data1, data1);
		TimberLogger.logAttributes(attr2, attr2);
		TimberLogger.logParams(LogParams.create().attr(attr3, attr3).metric(metric2, 2).data(data2, data2));
		return TimberLogger.getCurrentTaskId();
	}

	@Test
	public void testSimpleTaskWithTrimmer() {
		String attr = "attr";
		String data = "sql";

		StringBuilder sb = new StringBuilder();

		for (int i = 0 ; i < 1000000; i++){
			sb.append("a");
		}
		String hugeString = sb.toString();

		String taskId = testSimpleTaskWithTrimmer1(attr, data, hugeString);
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null)
				&& (client.getTaskById(taskId).getStatus() == TaskStatus.SUCCESS));

		Task task = client.getTaskById(taskId);
		assertEquals(EVENT, task.getTaskType());
		Map<String, Object> attributes = task.getAttributes();
		Map<String, String> datas = task.getData();
		assertEquals(10000, String.valueOf(attributes.get(attr)).length());
		assertEquals(1000, String.valueOf(datas.get(data)).length());
	}

	@TimberLog(taskType = EVENT)
	private String testSimpleTaskWithTrimmer1(String attr, String data, String hugeString) {
		TimberLogger.logAttributes(attr, hugeString);
		TimberLogger.logData(data, hugeString);
		return TimberLogger.getCurrentTaskId();
	}

	@Test
	public void testSpotWithParent(){
		String attr1 = "attr1";
		String attr2 = "attr2";

		final String[] taskIdSpot = {null};
		Pair<String, String> stringStringPair = testSpotWithParentTimberLog(attr1, attr2, taskIdSpot);

		String taskId1 = stringStringPair.getLeft();
		String taskId2 = stringStringPair.getRight();
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId1) != null)
				&& (client.getTaskById(taskId1).getStatus() == TaskStatus.SUCCESS));
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId1) != null)
				&& (client.getTaskById(taskId1).getStatus() == TaskStatus.SUCCESS));
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskIdSpot[0]) != null)
				&& (client.getTaskById(taskId2).getStatus() == TaskStatus.SUCCESS));

		Task task = client.getTaskById(taskId1);
		assertEquals(EVENT, task.getTaskType());
		assertEquals(taskId1, task.getPrimaryTaskId());
		assertEquals(TaskStatus.SUCCESS, task.getStatus());
		assertNull(task.getParentTaskId());

		Task spot = client.getTaskById(taskIdSpot[0]);
		assertEquals(SPOT, spot.getTaskType());
		assertEquals(attr1, spot.getAttributes().get(attr1));
		assertEquals(taskId1, spot.getPrimaryTaskId());
		assertEquals(TaskStatus.SUCCESS, spot.getStatus());
		assertEquals(taskId1, spot.getParentTaskId());
		assertEquals(EVENT, spot.getPrimaryOrigin());
		assertTrue(spot.getOrigins().contains(EVENT));
	}

	@com.datorama.timbermill.annotation.TimberLog(taskType = EVENT)
	private Pair<String, String> testSpotWithParentTimberLog(String attr1, String attr2, String[] taskIdSpot) {
		String taskId1 = TimberLogger.getCurrentTaskId();
		TimberLogger.logAttributes(attr1, attr1);
		String taskId2 = testSpotWithParentTimberLog2(attr2, taskIdSpot, taskId1);
		return Pair.of(taskId1, taskId2);
	}


	@com.datorama.timbermill.annotation.TimberLog(taskType = EVENT + '2')
	private String testSpotWithParentTimberLog2(String attr2, String[] taskIdSpot, String taskId1) {
		TimberLogger.logAttributes(attr2, attr2);
		new Thread(() -> {
			try(TimberLogContext tlc = new TimberLogContext(taskId1)){
				taskIdSpot[0] = TimberLogger.spot(SPOT);
			}
		}).run();
		return TimberLogger.getCurrentTaskId();
	}

	@Test
	public void testSimpleTasksFromDifferentThreadsIndexerJob(){

		String attr1 = "attr1";
		String attr2 = "attr2";

		String[] tasks = testSimpleTasksFromDifferentThreadsIndexerJobTimberLog1(attr1, attr2);
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
		assertEquals(EVENT, task.getTaskType());
		assertEquals(taskId, task.getPrimaryTaskId());
		assertEquals(TaskStatus.SUCCESS, task.getStatus());
		assertNull(task.getParentTaskId());

		Task task3 = client.getTaskById(taskId3);
		assertEquals(EVENT + '3', task3.getTaskType());
		assertEquals(attr1, task3.getAttributes().get(attr1));
		assertEquals(attr2, task3.getAttributes().get(attr2));
		assertEquals(taskId, task3.getPrimaryTaskId());
		assertEquals(TaskStatus.SUCCESS, task3.getStatus());
		assertEquals(taskId2, task3.getParentTaskId());
		assertEquals(EVENT, task3.getPrimaryOrigin());
		assertTrue(task3.getOrigins().contains(EVENT));
		assertTrue(task3.getOrigins().contains(EVENT + '2'));

		Task spot = client.getTaskById(taskIdSpot);
		assertEquals(SPOT, spot.getTaskType());
		assertEquals(attr1, spot.getAttributes().get(attr1));
		assertEquals(taskId, spot.getPrimaryTaskId());
		assertEquals(TaskStatus.SUCCESS, spot.getStatus());
		assertEquals(taskId, spot.getParentTaskId());
		assertEquals(EVENT, spot.getPrimaryOrigin());
		assertTrue(spot.getOrigins().contains(EVENT));
	}

	@com.datorama.timbermill.annotation.TimberLog(taskType = EVENT)
	private String[] testSimpleTasksFromDifferentThreadsIndexerJobTimberLog1(String attr1, String attr2) {
		TimberLogger.logAttributes(attr1, attr1);
		String taskId = TimberLogger.getCurrentTaskId();
		String[] tasks = testSimpleTasksFromDifferentThreadsIndexerJobTimberLog2(attr2, taskId);
		tasks[0] = TimberLogger.getCurrentTaskId();
		return tasks;
	}

	@com.datorama.timbermill.annotation.TimberLog(taskType = EVENT + '2')
	private String[] testSimpleTasksFromDifferentThreadsIndexerJobTimberLog2(String attr2, String taskId) {

		TimberLogger.logAttributes(attr2, attr2);

		final String[] tasks = new String[4];
		tasks[1] = TimberLogger.getCurrentTaskId();
		new Thread(() -> {
			try(TimberLogContext tlc = new TimberLogContext(tasks[1])){
				tasks[2] = testSimpleTasksFromDifferentThreadsIndexerJobTimberLog3();
			}
		}).run();

		new Thread(() -> {
			try(TimberLogContext tlc = new TimberLogContext(taskId)) {
				tasks[3] = testSimpleTasksFromDifferentThreadsIndexerJobTimberLog4();
			}
		}).run();
		return tasks;
	}

	@TimberLog(taskType = EVENT + '3')
	private String testSimpleTasksFromDifferentThreadsIndexerJobTimberLog3() {
		return TimberLogger.getCurrentTaskId();
	}

	@TimberLog(taskType = SPOT)
	private String testSimpleTasksFromDifferentThreadsIndexerJobTimberLog4() {
		return TimberLogger.getCurrentTaskId();
	}

	@Test
	public void testSimpleTasksFromDifferentThreadsWithWrongParentIdIndexerJob() {
		final String[] taskId = new String[1];
		new Thread(() -> {
			try(TimberLogContext tlc = new TimberLogContext("bla")){
				taskId[0] = testSimpleTasksFromDifferentThreadsWithWrongParentIdIndexerJobTimberLog();
			}

		}).run();



		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId[0]) != null)
				&& (client.getTaskById(taskId[0]).getStatus() == TaskStatus.SUCCESS));

		Task task = client.getTaskById(taskId[0]);
		assertEquals(EVENT, task.getTaskType());
		assertEquals(taskId[0], task.getPrimaryTaskId());
		assertEquals(TaskStatus.SUCCESS, task.getStatus());
		assertNull(task.getParentTaskId());
	}

	@TimberLog(taskType = EVENT)
	private String testSimpleTasksFromDifferentThreadsWithWrongParentIdIndexerJobTimberLog() {
		return TimberLogger.getCurrentTaskId();
	}

	@Test
	public void testComplexTaskIndexerWithErrorTask() {

		String parentTaskId = testComplexTaskIndexerWithErrorTaskTimberLog3();
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(parentTaskId) != null)
				&& (client.getTaskById(parentTaskId).getStatus() == TaskStatus.SUCCESS));
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> client.getTaskById(childTaskId) != null);
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> client.getTaskById(childOfChildTaskId) != null);

		Task taskParent = client.getTaskById(parentTaskId);
		Task taskChild = client.getTaskById(childTaskId);
		Task taskChildOfChild = client.getTaskById(childOfChildTaskId);

		assertSame(TaskStatus.SUCCESS, taskParent.getStatus());
		assertEquals(parentTaskId, taskParent.getTaskId());
		assertNull(taskParent.getOrigins());

		assertSame(TaskStatus.ERROR, taskChild.getStatus());
		assertEquals(childTaskId, taskChild.getTaskId());
		assertEquals(parentTaskId, taskChild.getPrimaryTaskId());
		assertEquals(parentTaskId, taskChild.getParentTaskId());
		assertEquals(1, taskChild.getOrigins().size());
		assertTrue(taskChild.getOrigins().contains(taskParent.getTaskType()));

		assertSame(TaskStatus.ERROR, taskChildOfChild.getStatus());
		assertEquals(childOfChildTaskId, taskChildOfChild.getTaskId());
		assertEquals(parentTaskId, taskChildOfChild.getPrimaryTaskId());
		assertEquals(childTaskId, taskChildOfChild.getParentTaskId());
		assertEquals(2, taskChildOfChild.getOrigins().size());
		assertEquals(taskChildOfChild.getOrigins().get(0), taskParent.getTaskType());
		assertEquals(taskChildOfChild.getOrigins().get(1), taskChild.getTaskType());
	}

	@com.datorama.timbermill.annotation.TimberLog(taskType = EVENT)
	private String testComplexTaskIndexerWithErrorTaskTimberLog3(){
		try {
			testComplexTaskIndexerWithErrorTaskTimberLog2();
		} catch (Exception ignored) {
		}
		return TimberLogger.getCurrentTaskId();
	}

	@com.datorama.timbermill.annotation.TimberLog(taskType = EVENT_CHILD)
	private void testComplexTaskIndexerWithErrorTaskTimberLog2() throws Exception {
		childTaskId = TimberLogger.getCurrentTaskId();
		testComplexTaskIndexerWithErrorTaskTimberLog1();
	}

	@com.datorama.timbermill.annotation.TimberLog(taskType = EVENT_CHILD_OF_CHILD)
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

	@com.datorama.timbermill.annotation.TimberLog(taskType = EVENT)
	private String testTaskWithNullStringTimberLog() {
		TimberLogger.logAttributes("key", "null");
		return TimberLogger.getCurrentTaskId();
	}

}
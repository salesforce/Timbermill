package com.datorama.timbermill;

import com.datorama.timbermill.annotation.TimberLog;
import com.datorama.timbermill.pipe.LocalOutputPipe;
import com.datorama.timbermill.pipe.LocalOutputPipeConfig;
import com.datorama.timbermill.unit.LogParams;
import com.datorama.timbermill.unit.Task;
import org.apache.commons.lang3.tuple.Pair;
import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.datorama.timbermill.TimberLogger.ENV;
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
	private static ElasticsearchClient client = new ElasticsearchClient(TEST, HTTP_LOCALHOST_9200, 1000,0, null);
	private String childTaskId;
	private String childOfChildTaskId;

	static final String LOG_REGEX = "\\[\\w\\w\\w \\d+, \\d\\d\\d\\d \\d+:\\d\\d:\\d\\d \\w\\w] \\[INFO] - ";

	@BeforeClass
	public static void init() {
		Map<String, Integer> map = new HashMap<>();
		map.put("text.sql1", 10000);
		map.put("string.sql1", 10000);
		map.put("ctx.sql1", 10000);
		map.put("text.sql2", 100);
		map.put("string.sql2", 100);
		map.put("ctx.sql2", 100);

        LocalOutputPipeConfig config = new LocalOutputPipeConfig.Builder().env(TEST).url(HTTP_LOCALHOST_9200).defaultMaxChars(1000).propertiesLengthMap(map).secondBetweenPolling(1)
				.pluginJson("[{\"class\":\"SwitchCasePlugin\",\"taskMatcher\":{\"name\":\""+ EVENT + "plugin" + "\"},\"searchField\":\"exception\",\"outputAttribute\":\"errorType\",\"switchCase\":[{\"match\":[\"TOO_MANY_SERVER_ROWS\"],\"output\":\"TOO_MANY_SERVER_ROWS\"},{\"match\":[\"PARAMETER_MISSING\"],\"output\":\"PARAMETER_MISSING\"},{\"match\":[\"Connections could not be acquired\",\"terminating connection due to administrator\",\"connect timed out\"],\"output\":\"DB_CONNECT\"},{\"match\":[\"did not fit in memory\",\"Insufficient resources to execute plan\",\"Query exceeded local memory limit\",\"ERROR: Plan memory limit exhausted\"],\"output\":\"DB_RESOURCES\"},{\"match\":[\"Invalid input syntax\",\"SQLSyntaxErrorException\",\"com.facebook.presto.sql.parser.ParsingException\",\"com.facebook.presto.sql.analyzer.SemanticException\",\"org.postgresql.util.PSQLException: ERROR: missing FROM-clause entry\",\"org.postgresql.util.PSQLException: ERROR: invalid input syntax\"],\"output\":\"DB_SQL_SYNTAX\"},{\"match\":[\"Execution canceled by operator\",\"InterruptedException\",\"Execution time exceeded run time cap\",\"TIME_OUT\",\"canceling statement due to user request\",\"Caused by: java.net.SocketTimeoutException: Read timed out\"],\"output\":\"DB_QUERY_TIME_OUT\"},{\"output\":\"DB_UNKNOWN\"}]}]")
                .build();
		TimberLogger.bootstrap(new LocalOutputPipe(config));
	}

	@AfterClass
	public static void kill() {
		TimberLogger.exit();
	}

	@Test
	public void testSimpleTaskIndexerJob() throws InterruptedException {
		String str1 = "str1";
		String str2 = "str2";
		String metric1 = "metric1";
		String metric2 = "metric2";
		String text1 = "text1";
		String text2 = "text2";

		String log1 = "log1";
		String log2 = "log2";
		String taskId = testSimpleTaskIndexerJobTimberLog(str1, str2, metric1, metric2, text1, text2, log1, log2);

		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null)
				&& (client.getTaskById(taskId).getStatus() == TaskStatus.SUCCESS));
		Task task = client.getTaskById(taskId);

		assertTaskPrimary(task, EVENT, TaskStatus.SUCCESS, taskId, true, true);
		assertEquals(TEST, task.getCtx().get(ENV));
		assertTrue(task.getDuration() > 1000);
		Map<String, String> strings = task.getString();
		Map<String, Number> metrics = task.getMetric();
		Map<String, String> texts = task.getText();
		String log = task.getLog();

		assertEquals(str1, strings.get(str1));
		assertEquals(str2, strings.get(str2));
		assertEquals(1, metrics.get(metric1).intValue());
		assertEquals(2, metrics.get(metric2).intValue());
		assertEquals(text1, texts.get(text1));
		assertEquals(text2, texts.get(text2));

		String[] split = log.split("\n");
		assertEquals(2, split.length);
		assertTrue(split[0].matches(LOG_REGEX + log1));
		assertTrue(split[1].matches(LOG_REGEX + log2));
	}

	@TimberLog(name = EVENT)
	private String testSimpleTaskIndexerJobTimberLog(String str1, String str2, String metric1, String metric2, String text1, String text2, String log1, String log2) throws InterruptedException {
		TimberLogger.logString(str1, str1);
		TimberLogger.logMetric(metric1, 1);
		TimberLogger.logText(text1, text1);
		TimberLogger.logInfo(log1);
		TimberLogger.logParams(LogParams.create().string(str2, str2).metric(metric2, 2).text(text2, text2).logInfo(log2));
		Thread.sleep(1000);
		return TimberLogger.getCurrentTaskId();
	}

	@Test
	public void testSwitchCasePlugin() {

		String taskId = testSwitchCasePluginLog();

		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null)
				&& (client.getTaskById(taskId).getStatus() == TaskStatus.SUCCESS));
		Task task = client.getTaskById(taskId);

        assertTaskPrimary(task, EVENT + "plugin", TaskStatus.SUCCESS, taskId, true, true);
		Map<String, String> strings = task.getString();
		String errorType = strings.get("errorType");
		assertEquals("TOO_MANY_SERVER_ROWS", errorType);
	}

	@TimberLog(name = EVENT + "plugin")
	private String testSwitchCasePluginLog() {
		TimberLogger.logText("exception", "bla bla bla TOO_MANY_SERVER_ROWS bla bla bla");
		return TimberLogger.getCurrentTaskId();
	}

	@Test
	public void testSimpleTaskWithTrimmer() {

		StringBuilder sb = new StringBuilder();

		for (int i = 0 ; i < 1000000; i++){
			sb.append("a");
		}
		String hugeString = sb.toString();

		String taskId = testSimpleTaskWithTrimmer1(hugeString);
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null)
				&& (client.getTaskById(taskId).getStatus() == TaskStatus.SUCCESS));

		Task task = client.getTaskById(taskId);

        assertTaskPrimary(task, EVENT, TaskStatus.SUCCESS, taskId, true, true);
		Map<String, String> strings = task.getString();
		Map<String, String> texts = task.getText();
		Map<String, String> context = task.getCtx();
		assertEquals(10000,strings.get("sql1").length());
		assertEquals(100, strings.get("sql2").length());
		assertEquals(1000, strings.get("sql3").length());
		assertEquals(10000, texts.get("sql1").length());
		assertEquals(100, texts.get("sql2").length());
		assertEquals(1000, texts.get("sql3").length());
		assertEquals(10000, context.get("sql1").length());
		assertEquals(100, context.get("sql2").length());
		assertEquals(1000, context.get("sql3").length());
	}

	@TimberLog(name = EVENT)
	private String testSimpleTaskWithTrimmer1(String hugeString) {
		TimberLogger.logString("sql1", hugeString);
		TimberLogger.logString("sql2", hugeString);
		TimberLogger.logString("sql3", hugeString);
		TimberLogger.logText("sql1", hugeString);
		TimberLogger.logText("sql2", hugeString);
		TimberLogger.logText("sql3", hugeString);
		TimberLogger.logContext("sql1", hugeString);
		TimberLogger.logContext("sql2", hugeString);
		TimberLogger.logContext("sql3", hugeString);
		return TimberLogger.getCurrentTaskId();
	}

	@Test
	public void testSpotWithParent(){
		String context = "context";
		String str = "str";

		final String[] taskIdSpot = {null};
		Pair<String, String> stringStringPair = testSpotWithParentTimberLog(context, str, taskIdSpot);

		String taskId1 = stringStringPair.getLeft();
		String taskId2 = stringStringPair.getRight();
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId1) != null)
				&& (client.getTaskById(taskId1).getStatus() == TaskStatus.SUCCESS));
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId1) != null)
				&& (client.getTaskById(taskId1).getStatus() == TaskStatus.SUCCESS));
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskIdSpot[0]) != null)
				&& (client.getTaskById(taskId2).getStatus() == TaskStatus.SUCCESS));

		Task task = client.getTaskById(taskId1);
        assertTaskPrimary(task, EVENT, TaskStatus.SUCCESS, taskId1, true, true);

		Task spot = client.getTaskById(taskIdSpot[0]);
		assertTask(spot, SPOT, true, true, taskId1, taskId1, TaskStatus.SUCCESS, false, EVENT);
		assertEquals(context, spot.getCtx().get(context));

        Task child = client.getTaskById(taskId2);
        assertTask(child, EVENT + '2', true, true, taskId1, taskId1, TaskStatus.SUCCESS, false, EVENT);
        assertEquals(context, child.getCtx().get(context));
	}

	@TimberLog(name = EVENT)
	private Pair<String, String> testSpotWithParentTimberLog(String context1, String context2, String[] taskIdSpot) {
		String taskId1 = TimberLogger.getCurrentTaskId();
		TimberLogger.logContext(context1, context1);
		String taskId2 = testSpotWithParentTimberLog2(context2, taskIdSpot, taskId1);
		return Pair.of(taskId1, taskId2);
	}


	@TimberLog(name = EVENT + '2')
	private String testSpotWithParentTimberLog2(String context2, String[] taskIdSpot, String taskId1) {
		TimberLogger.logContext(context2, context2);
		Thread thread = new Thread(() -> {
			try (TimberLogContext ignored = new TimberLogContext(taskId1)) {
				taskIdSpot[0] = TimberLogger.spot(SPOT);
			}
		});
		thread.start();
		while(thread.isAlive()){}
		return TimberLogger.getCurrentTaskId();
	}

	@Test
	public void testSimpleTasksFromDifferentThreadsIndexerJob(){

		String context1 = "context1";
		String context2 = "context2";

		String[] tasks = testSimpleTasksFromDifferentThreadsIndexerJobTimberLog1(context1, context2);
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
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskIdSpot) != null)
				&& (client.getTaskById(taskIdSpot).getStatus() == TaskStatus.SUCCESS));


		Task task = client.getTaskById(taskId);
		assertTaskPrimary(task, EVENT, TaskStatus.SUCCESS, taskId, true, true);
        assertEquals(context1, task.getCtx().get(context1));

        Task task2 = client.getTaskById(taskId2);
        assertTask(task2, EVENT + '2', true, true, taskId, taskId, TaskStatus.SUCCESS, false, EVENT);
        assertEquals(context1, task2.getCtx().get(context1));
        assertEquals(context2, task2.getCtx().get(context2));

		Task task3 = client.getTaskById(taskId3);
        assertTask(task3, EVENT + '3', true, true, taskId, taskId2, TaskStatus.SUCCESS, false, EVENT, EVENT + '2');
		assertEquals(context1, task3.getCtx().get(context1));
		assertEquals(context2, task3.getCtx().get(context2));

		Task spot = client.getTaskById(taskIdSpot);
		assertTask(spot, SPOT, true, true, taskId, taskId, TaskStatus.SUCCESS, false, EVENT);
		assertEquals(context1, spot.getCtx().get(context1));
	}

	@TimberLog(name = EVENT)
	private String[] testSimpleTasksFromDifferentThreadsIndexerJobTimberLog1(String context1, String context2) {
		TimberLogger.logContext(context1, context1);
		String taskId = TimberLogger.getCurrentTaskId();
		String[] tasks = testSimpleTasksFromDifferentThreadsIndexerJobTimberLog2(context2, taskId);
		tasks[0] = TimberLogger.getCurrentTaskId();
		return tasks;
	}

	@TimberLog(name = EVENT + '2')
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
		while(thread1.isAlive()){
		}
		while(thread2.isAlive()){
		}
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
		Thread thread = new Thread(() -> {
			try (TimberLogContext ignored = new TimberLogContext("bla")) {
				taskId[0] = testSimpleTasksFromDifferentThreadsWithWrongParentIdIndexerJobTimberLog();
			}

		});
		thread.start();
		while(thread.isAlive()){}



		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId[0]) != null)
				&& (client.getTaskById(taskId[0]).getStatus() == TaskStatus.SUCCESS));

		Task task = client.getTaskById(taskId[0]);
		assertTask(task, EVENT, true, true, null, "bla", TaskStatus.SUCCESS, false);
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

		assertTaskPrimary(taskParent, EVENT, TaskStatus.SUCCESS, parentId, true, true);

		assertTask(taskChild, EVENT_CHILD, true, true, parentId, parentId, TaskStatus.ERROR, false, EVENT);

		assertTask(taskChildOfChild, EVENT_CHILD_OF_CHILD, true, true, parentId, childTaskId, TaskStatus.ERROR, false, EVENT, EVENT_CHILD);
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
		assertTaskPrimary(task, EVENT, TaskStatus.SUCCESS, taskId, true, true);
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
		assertTaskPrimary(task, "ctr", TaskStatus.SUCCESS, taskId, true, true);
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
		assertTaskPrimary(task, "ctr", TaskStatus.ERROR, taskId, true, true);
	}

    static void assertTask(Task task, String name, boolean shouldHaveEndTime, boolean shouldBeComplete, String primaryId, String parentId, TaskStatus status, Boolean isPrimary, String... parents) {
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

		if (isPrimary == null){
            assertNull(task.isPrimary());
        }
        else {
            assertEquals(isPrimary, task.isPrimary());
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
        assertTask(task, name, shouldHaveEndTime, shouldBeComplete, primaryId, null, status, true);
    }

	static void assertTaskCorrupted(Task task, String name, TaskStatus status, boolean shouldHaveEndTime) {
		assertTask(task, name, shouldHaveEndTime, false, null, null, status, null);
	}
}
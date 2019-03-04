package com.datorama.timbermill;

import com.datorama.timbermill.pipe.LocalOutputPipeConfig;
import com.datorama.timbermill.unit.Task;
import com.jayway.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
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

	@BeforeClass
	public static void init() {
		Map<String, Integer> map = Collections.singletonMap("data.sql", 1000);
		LocalOutputPipeConfig.Builder builder = new LocalOutputPipeConfig.Builder().env(TEST).url(HTTP_LOCALHOST_9200).defaultMaxChars(10000).propertiesLengthMap(map).secondBetweenPolling(1);
		LocalOutputPipeConfig config = new LocalOutputPipeConfig(builder);
		TimberLog.bootstrap(config);
	}

	@AfterClass
	public static void kill() {
		TimberLog.exit();
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

		String taskId = TimberLog.start(EVENT);
		try{
			TimberLog.logAttributes(attr1, attr1);
			TimberLog.logMetrics(metric1, 1);
			TimberLog.logData(data1, data1);
			TimberLog.logAttributes(attr2, attr2);
			TimberLog.logParams(LogParams.create().attr(attr3, attr3).metric(metric2, 2).data(data2, data2));
			TimberLog.success();
		} catch (Throwable t){
			TimberLog.error(t);
			throw t;
		}

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

	@Test
	public void testSimpleTaskWithTrimmer() {
		String attr = "attr";
		String data = "sql";

		StringBuilder sb = new StringBuilder();

		for (int i = 0 ; i < 1000000; i++){
			sb.append("a");
		}
		String hugeString = sb.toString();

		String taskId = TimberLog.start(EVENT);
		try{
			TimberLog.logAttributes(attr, hugeString);
			TimberLog.logData(data, hugeString);
			TimberLog.success();
		} catch (Throwable t){
			TimberLog.error(t);
			throw t;
		}
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null)
				&& (client.getTaskById(taskId).getStatus() == TaskStatus.SUCCESS));

		Task task = client.getTaskById(taskId);
		assertEquals(EVENT, task.getTaskType());
		Map<String, Object> attributes = task.getAttributes();
		Map<String, String> datas = task.getData();
		assertEquals(10000, String.valueOf(attributes.get(attr)).length());
		assertEquals(1000, String.valueOf(datas.get(data)).length());
	}

	@Test
	public void testSpotWithParent(){
		String attr1 = "attr1";
		String attr2 = "attr2";

		final String[] taskIdSpot = {null};
		String taskId2;
		String taskId = TimberLog.start(EVENT);
		try {
			TimberLog.logAttributes(attr1, attr1);

			taskId2 = TimberLog.start(EVENT + '2');
			try{
				TimberLog.logAttributes(attr2, attr2);
				new Thread(() -> {
					try {
						taskIdSpot[0] = TimberLog.spot(SPOT, LogParams.create(), taskId);
					} catch (Exception e) {
						fail();
					}
				}).run();
				TimberLog.success();
			} catch (Throwable t){
				TimberLog.error(t);
				throw t;
			}
			TimberLog.success();
		} catch (Throwable t){
			TimberLog.error(t);
			throw t;
		}

		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null)
				&& (client.getTaskById(taskId).getStatus() == TaskStatus.SUCCESS));
		String finalTaskId1 = taskId2;
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(finalTaskId1) != null)
				&& (client.getTaskById(finalTaskId1).getStatus() == TaskStatus.SUCCESS));
		String finalTaskId2 = taskId2;
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskIdSpot[0]) != null)
				&& (client.getTaskById(finalTaskId2).getStatus() == TaskStatus.SUCCESS));

		Task task = client.getTaskById(taskId);
		assertEquals(EVENT, task.getTaskType());
		assertEquals(taskId, task.getPrimaryTaskId());
		assertEquals(TaskStatus.SUCCESS, task.getStatus());
		assertNull(task.getParentTaskId());

		Task spot = client.getTaskById(taskIdSpot[0]);
		assertEquals(SPOT, spot.getTaskType());
		assertEquals(attr1, spot.getAttributes().get(attr1));
		assertEquals(taskId, spot.getPrimaryTaskId());
		assertEquals(TaskStatus.SUCCESS, spot.getStatus());
		assertEquals(taskId, spot.getParentTaskId());
		assertEquals(EVENT, spot.getPrimaryOrigin());
		assertTrue(spot.getOrigins().contains(EVENT));
	}

	@Test
	public void testSimpleTasksFromDifferentThreadsIndexerJob(){

		String attr1 = "attr1";
		String attr2 = "attr2";

		String taskId2;
		final String[] taskId3 = new String[1];
		final String[] taskIdSpot = new String[1];
		String taskId = TimberLog.start(EVENT);
		try{
			TimberLog.logAttributes(attr1, attr1);

			taskId2 = TimberLog.start(EVENT + '2');
			try{

				TimberLog.logAttributes(attr2, attr2);

				new Thread(() -> {
					try {
						taskId3[0] = TimberLog.start(EVENT + '3', taskId2);
						try{
							TimberLog.success();
						} catch (Throwable t){
							TimberLog.error(t);
							throw t;
						}
					} catch (Exception e) {
						fail();
					}
				}).run();

				new Thread(() -> {
					try {
						taskIdSpot[0] = TimberLog.start(SPOT, taskId);
						try{
							TimberLog.success();
						} catch (Throwable t){
							TimberLog.error(t);
							throw t;
						}
					} catch (Exception e) {
						fail();
					}
				}).run();
				TimberLog.success();
			} catch (Throwable t){
				TimberLog.error(t);
				throw t;
			}
			TimberLog.success();
		} catch (Throwable t){
			TimberLog.error(t);
			throw t;
		}

		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null)
				&& (client.getTaskById(taskId).getStatus() == TaskStatus.SUCCESS));
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId2) != null)
				&& (client.getTaskById(taskId2).getStatus() == TaskStatus.SUCCESS));
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId3[0]) != null)
				&& (client.getTaskById(taskId3[0]).getStatus() == TaskStatus.SUCCESS));
		Task task = client.getTaskById(taskId);
		assertEquals(EVENT, task.getTaskType());
		assertEquals(taskId, task.getPrimaryTaskId());
		assertEquals(TaskStatus.SUCCESS, task.getStatus());
		assertNull(task.getParentTaskId());

		Task task3 = client.getTaskById(taskId3[0]);
		assertEquals(EVENT + '3', task3.getTaskType());
		assertEquals(attr1, task3.getAttributes().get(attr1));
		assertEquals(attr2, task3.getAttributes().get(attr2));
		assertEquals(taskId, task3.getPrimaryTaskId());
		assertEquals(TaskStatus.SUCCESS, task3.getStatus());
		assertEquals(taskId2, task3.getParentTaskId());
		assertEquals(EVENT, task3.getPrimaryOrigin());
		assertTrue(task3.getOrigins().contains(EVENT));
		assertTrue(task3.getOrigins().contains(EVENT + '2'));

		Task spot = client.getTaskById(taskIdSpot[0]);
		assertEquals(SPOT, spot.getTaskType());
		assertEquals(attr1, spot.getAttributes().get(attr1));
		assertEquals(taskId, spot.getPrimaryTaskId());
		assertEquals(TaskStatus.SUCCESS, spot.getStatus());
		assertEquals(taskId, spot.getParentTaskId());
		assertEquals(EVENT, spot.getPrimaryOrigin());
		assertTrue(spot.getOrigins().contains(EVENT));
	}

	@Test
	public void testSimpleTasksFromDifferentThreadsWithWrongParentIdIndexerJob() {
		final String[] taskIdArr = new String[1];
		new Thread(() -> {
			try {
				taskIdArr[0] = TimberLog.start(EVENT, "bla");
				try{
					TimberLog.success();
				} catch (Throwable t){
					TimberLog.error(t);
					throw t;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}).run();

		String taskId = taskIdArr[0];

		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null)
				&& (client.getTaskById(taskId).getStatus() == TaskStatus.SUCCESS));

		Task task = client.getTaskById(taskId);
		assertEquals(EVENT, task.getTaskType());
		assertEquals(taskId, task.getPrimaryTaskId());
		assertEquals(TaskStatus.SUCCESS, task.getStatus());
		assertNull(task.getParentTaskId());

	}

	@Test
	public void testComplexTaskIndexerWithErrorTask() {

		String childTaskId = null;
		String childOfChildTaskId = null;
		String parentTaskId = TimberLog.start(EVENT);
		try{
			try {
				TimberLog.start(EVENT_CHILD);
				try{
					childTaskId = TimberLog.getCurrentTaskId();
					try {
						TimberLog.start(EVENT_CHILD_OF_CHILD);
						try{
							childOfChildTaskId = TimberLog.getCurrentTaskId();
							throw FAIL;
						} catch (Throwable t){
							TimberLog.error(t);
							throw t;
						}
					} catch (Exception ignored) {}
					throw FAIL;
				} catch (Throwable t){
					TimberLog.error(t);
					throw t;
				}
			} catch (Exception ignored) {}
			TimberLog.success();
		} catch (Throwable t){
			TimberLog.error(t);
			throw t;
		}
		String finalChildTaskId = childTaskId;
		String finalChildOfChildTaskId = childOfChildTaskId;
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(parentTaskId) != null)
				&& (client.getTaskById(parentTaskId).getStatus() == TaskStatus.SUCCESS));
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> client.getTaskById(finalChildTaskId) != null);
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> client.getTaskById(finalChildOfChildTaskId) != null);

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

	@Test
	public void testTaskWithNullString() {

		String taskId = TimberLog.start(EVENT);
		try{
			TimberLog.logAttributes("key", "null");
			TimberLog.success();
		} catch (Throwable t){
			TimberLog.error(t);
			throw t;
		}
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null)
				&& (client.getTaskById(taskId).getStatus() == TaskStatus.SUCCESS));

		Task task = client.getTaskById(taskId);
		assertSame(TaskStatus.SUCCESS, task.getStatus());
	}

}
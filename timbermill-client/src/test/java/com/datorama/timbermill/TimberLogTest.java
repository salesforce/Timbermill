package com.datorama.timbermill;

import com.datorama.timbermill.pipe.LocalOutputPipeConfig;
import com.datorama.timbermill.unit.Task;
import com.jayway.awaitility.Awaitility;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.datorama.timbermill.unit.Task.*;
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
	public static void kill() throws IOException {
		TimberLog.stop();
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

		String taskId;
		TimberLog.start(EVENT);
		try{
			TimberLog.logAttributes(attr1, attr1);
			TimberLog.logMetrics(metric1, 1);
			TimberLog.logData(data1, data1);
			TimberLog.logAttributes(attr2, attr2);
			TimberLog.logParams(LogParams.create().attr(attr3, attr3).metric(metric2, 2).data(data2, data2));
			taskId = TimberLog.getCurrentTaskId();
			TimberLog.success();
		} catch (Throwable t){
			TimberLog.error(t);
			throw t;
		}

		String finalTaskId = taskId;
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(finalTaskId) != null)
				&& (client.getTaskById(finalTaskId).getStatus() == TaskStatus.SUCCESS));
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

		String taskId;
		TimberLog.start(EVENT);
		try{
			TimberLog.logAttributes(attr, hugeString);
			TimberLog.logData(data, hugeString);
			taskId = TimberLog.getCurrentTaskId();
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

		String taskId;
		String taskId2;
		final String[] taskIdSpot = {null};

		TimberLog.start(EVENT);
		try {
			taskId = TimberLog.getCurrentTaskId();
			TimberLog.logAttributes(attr1, attr1);

			TimberLog.start(EVENT + '2');
			try{
				taskId2 = TimberLog.getCurrentTaskId();
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

		String finalTaskId = taskId;
		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(finalTaskId) != null)
				&& (client.getTaskById(finalTaskId).getStatus() == TaskStatus.SUCCESS));
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
		final String[] taskId1Arr = new String[1];
		final String[] taskId2Arr = new String[1];

		final String[] task3IdArr = new String[1];
		final String[] taskSpotArr = new String[1];

		String attr1 = "attr1";
		String attr2 = "attr2";

		TimberLog.start(EVENT);
		try{
			taskId1Arr[0] = TimberLog.getCurrentTaskId();
			TimberLog.logAttributes(attr1, attr1);

			TimberLog.start(EVENT + '2');
			try{
				taskId2Arr[0] = TimberLog.getCurrentTaskId();

				TimberLog.logAttributes(attr2, attr2);

				new Thread(() -> {
					try {
						TimberLog.start(EVENT + '3', taskId2Arr[0]);
						try{
							task3IdArr[0] = TimberLog.getCurrentTaskId();
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
						TimberLog.start(SPOT, taskId1Arr[0]);
						try{
							taskSpotArr[0] = TimberLog.getCurrentTaskId();
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

		String taskId = taskId1Arr[0];
		String taskId2 = taskId2Arr[0];
		String taskId3 = task3IdArr[0];
		String taskIdSpot = taskSpotArr[0];

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

	@Test
	public void testSimpleTasksFromDifferentThreadsWithWrongParentIdIndexerJob() {
		final String[] taskIdArr = new String[1];
		new Thread(() -> {
			try {
				TimberLog.start(EVENT, "bla");
				try{
					taskIdArr[0] = TimberLog.getCurrentTaskId();
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
		String[] taskIds = new String[3];

		TimberLog.start(EVENT);
		try{
			try {
				taskIds[0] = TimberLog.getCurrentTaskId();
				TimberLog.start(EVENT_CHILD);
				try{
					taskIds[1] = TimberLog.getCurrentTaskId();
					try {
						TimberLog.start(EVENT_CHILD_OF_CHILD);
						try{
							taskIds[2] = TimberLog.getCurrentTaskId();
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

		String parentTaskId = taskIds[0];
		String childTaskId = taskIds[1];
		String childOfChildTaskId = taskIds[2];

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

	@Test
	public void testTaskWithNullString() {
		String[] taskIds = new String[1];

		TimberLog.start(EVENT);
		try{
			taskIds[0] = TimberLog.getCurrentTaskId();
			TimberLog.logAttributes("key", "null");
			TimberLog.success();
		} catch (Throwable t){
			TimberLog.error(t);
			throw t;
		}
		String taskId = taskIds[0];

		Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(2000, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null)
				&& (client.getTaskById(taskId).getStatus() == TaskStatus.SUCCESS));

		Task task = client.getTaskById(taskId);
		assertSame(TaskStatus.SUCCESS, task.getStatus());
	}

}
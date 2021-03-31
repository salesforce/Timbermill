package com.datorama.oss.timbermill;

import com.datorama.oss.timbermill.unit.Event;
import com.datorama.oss.timbermill.unit.LogParams;
import com.datorama.oss.timbermill.unit.Task;
import com.datorama.oss.timbermill.unit.TaskStatus;
import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

public class TasksMergerWithCacheTest extends TimberLogTest {

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

	@BeforeClass
	public static void init() {
		TimberLogLocalTest.init();
	}

	@Test
	public void testStartSuccessDifferentIndex() {

		LogParams startLogParams = LogParams.create().context(CTX_1, CTX_1).metric(METRIC_1,1).text(TEXT_1, TEXT_1).string(STRING_1, STRING_1);
		String id = TimberLogger.start(ROLLOVER_TEST, startLogParams);

		TimberLogTest.waitForTask(id, TaskStatus.UNTERMINATED);

		client.rolloverIndexForTest(TEST);

		LogParams successLogParams = LogParams.create().context(CTX_2, CTX_2).metric(METRIC_2,2).text(TEXT_2, TEXT_2).string(STRING_2, STRING_2);
		TimberLogger.logParams(successLogParams);
		TimberLogger.success();

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
	public void testStartInfoSuccessDifferentIndex() {

		LogParams startLogParams = LogParams.create().context(CTX_1, CTX_1).metric(METRIC_1,1).text(TEXT_1, TEXT_1).string(STRING_1, STRING_1);
		LogParams infoLogParams = LogParams.create().context(CTX_3, CTX_3).metric(METRIC_3,3).text(TEXT_3, TEXT_3).string(STRING_3, STRING_3);

		String id = TimberLogger.start(ROLLOVER_TEST, startLogParams);
		TimberLogger.logParams(infoLogParams);

		TimberLogTest.waitForTask(id, TaskStatus.UNTERMINATED);
		client.rolloverIndexForTest(TEST);

		LogParams successLogParams = LogParams.create().context(CTX_2, CTX_2).metric(METRIC_2,2).text(TEXT_2, TEXT_2).string(STRING_2, STRING_2);
		TimberLogger.logParams(successLogParams);
		TimberLogger.success();

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
	public void testSuccessStartDifferentIndex() {
		String id = Event.generateTaskId(ROLLOVER_TEST);
		LogParams successLogParams = LogParams.create().context(CTX_2, CTX_2).metric(METRIC_2,2).text(TEXT_2, TEXT_2).string(STRING_2, STRING_2);
		TimberLoggerAdvanced.success(id, successLogParams);

		TimberLogTest.waitForTask(id, TaskStatus.PARTIAL_SUCCESS);
		client.rolloverIndexForTest(TEST);

		LogParams startLogParams = LogParams.create().context(CTX_1, CTX_1).metric(METRIC_1,1).text(TEXT_1, TEXT_1).string(STRING_1, STRING_1);
		TimberLoggerAdvanced.start(id, ROLLOVER_TEST, null, startLogParams);

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
	public void testStartErrorDifferentIndex() {
		LogParams startLogParams = LogParams.create().context(CTX_1, CTX_1).metric(METRIC_1,1).text(TEXT_1, TEXT_1).string(STRING_1, STRING_1);
		String id = TimberLogger.start(ROLLOVER_TEST, startLogParams);

		TimberLogTest.waitForTask(id, TaskStatus.UNTERMINATED);
		client.rolloverIndexForTest(TEST);

		LogParams errorLogParams = LogParams.create().context(CTX_2, CTX_2).metric(METRIC_2,2).text(TEXT_2, TEXT_2).string(STRING_2, STRING_2);
		TimberLogger.logParams(errorLogParams);
		TimberLogger.error(new RuntimeException());

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
	public void testStartInfoErrorDifferentIndex() {
		LogParams startLogParams = LogParams.create().context(CTX_1, CTX_1).metric(METRIC_1,1).text(TEXT_1, TEXT_1).string(STRING_1, STRING_1);
		LogParams infoLogParams = LogParams.create().context(CTX_3, CTX_3).metric(METRIC_3,3).text(TEXT_3, TEXT_3).string(STRING_3, STRING_3);

		String id = TimberLogger.start(ROLLOVER_TEST, startLogParams);
		TimberLogger.logParams(infoLogParams);

		TimberLogTest.waitForTask(id, TaskStatus.UNTERMINATED);
		client.rolloverIndexForTest(TEST);

		LogParams errorLogParams = LogParams.create().context(CTX_2, CTX_2).metric(METRIC_2,2).text(TEXT_2, TEXT_2).string(STRING_2, STRING_2);
		TimberLogger.logParams(errorLogParams);
		TimberLogger.error(new RuntimeException());

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
	public void testErrorStartDifferentIndex() {
		String id = Event.generateTaskId(ROLLOVER_TEST);
		LogParams errorLogParams = LogParams.create().context(CTX_2, CTX_2).metric(METRIC_2,2).text(TEXT_2, TEXT_2).string(STRING_2, STRING_2);

		TimberLoggerAdvanced.error(id, new RuntimeException(), errorLogParams);

		TimberLogTest.waitForTask(id, TaskStatus.PARTIAL_ERROR);
		client.rolloverIndexForTest(TEST);

		LogParams startLogParams = LogParams.create().context(CTX_1, CTX_1).metric(METRIC_1,1).text(TEXT_1, TEXT_1).string(STRING_1, STRING_1);
		TimberLoggerAdvanced.start(id, ROLLOVER_TEST, null, startLogParams);

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
		String id = Event.generateTaskId(ROLLOVER_TEST);

		LogParams startLogParams = LogParams.create().context(CTX_1, CTX_1).metric(METRIC_1,1).text(TEXT_1, TEXT_1).string(STRING_1, STRING_1);
		TimberLoggerAdvanced.start(id, ROLLOVER_TEST, null, startLogParams);

		Thread.sleep(10);

		LogParams successLogParams = LogParams.create().context(CTX_2, CTX_2).metric(METRIC_2,2).text(TEXT_2, TEXT_2).string(STRING_2, STRING_2);
		TimberLoggerAdvanced.success(id, successLogParams);

		TimberLogTest.waitForTask(id, TaskStatus.SUCCESS);
		client.rolloverIndexForTest(TEST);

		LogParams infoLogParams = LogParams.create().context(CTX_3, CTX_3).metric(METRIC_3,3).text(TEXT_3, TEXT_3).string(STRING_3, STRING_3);
		TimberLoggerAdvanced.logParams(id, infoLogParams);

		Thread.sleep(3000);

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
		String id = Event.generateTaskId(ROLLOVER_TEST);

		LogParams startLogParams = LogParams.create().context(CTX_1, CTX_1).metric(METRIC_1,1).text(TEXT_1, TEXT_1).string(STRING_1, STRING_1);
		TimberLoggerAdvanced.start(id, ROLLOVER_TEST, null, startLogParams);

		Thread.sleep(10);

		LogParams errorLogParams = LogParams.create().context(CTX_2, CTX_2).metric(METRIC_2,2).text(TEXT_2, TEXT_2).string(STRING_2, STRING_2);
		TimberLoggerAdvanced.error(id, new RuntimeException(), errorLogParams);

		TimberLogTest.waitForTask(id, TaskStatus.ERROR);
		client.rolloverIndexForTest(TEST);

		LogParams infoLogParams = LogParams.create().context(CTX_3, CTX_3).metric(METRIC_3,3).text(TEXT_3, TEXT_3).string(STRING_3, STRING_3);
		TimberLoggerAdvanced.logParams(id, infoLogParams);

		Thread.sleep(3000);
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
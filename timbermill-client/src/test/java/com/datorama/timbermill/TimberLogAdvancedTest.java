package com.datorama.timbermill;

import com.datorama.timbermill.annotation.TimberLog;
import com.datorama.timbermill.unit.Event;
import com.datorama.timbermill.unit.LogParams;
import com.datorama.timbermill.unit.Task;
import org.awaitility.Awaitility;
import org.junit.AfterClass;

import java.util.concurrent.TimeUnit;

import static com.datorama.timbermill.TimberLogTest.*;
import static com.datorama.timbermill.unit.Task.TaskStatus;
import static org.junit.Assert.*;

public class TimberLogAdvancedTest {

    @AfterClass
    public static void kill() {
        TimberLogger.exit();
    }

    public void testOngoingTask() {
        final String[] taskId1Arr = new String[1];
        final String[] taskId2Arr = new String[1];
        final String[] ongoingTaskIdArr = new String[1];

        String ctx = "ctx";
        String ctx1 = "ctx1";
        String ctx2 = "ctx2";
        String ctx3 = "ctx3";
        String metric1 = "metric1";
        String metric2 = "metric2";
        String metric3 = "metric3";
        String text1 = "text1";
        String text2 = "text2";
        String text3 = "text3";
        String string1 = "string1";
        String string2 = "string2";
        String string3 = "string3";
        String log1 = "log1";
        String log2 = "log2";
        String log3 = "log3";
        String ongoingTaskName = EVENT + '1';

        testOngoingTask1(taskId1Arr, taskId2Arr, ongoingTaskIdArr,ctx, ctx1, ctx2, ctx3, metric1, metric2, metric3, text1, text2, text3, string1, string2, string3, log1, log2, log3, ongoingTaskName);


        String taskId = taskId1Arr[0];
        String childTaskId = taskId2Arr[0];
        String ongoingTaskId = ongoingTaskIdArr[0];

        Awaitility.await().atMost(90, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null)
                && (client.getTaskById(taskId).getStatus() == TaskStatus.SUCCESS));
        Awaitility.await().atMost(90, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(childTaskId) != null)
                && (client.getTaskById(childTaskId).getStatus() == TaskStatus.SUCCESS));
        Awaitility.await().atMost(90, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(ongoingTaskId) != null)
                && (client.getTaskById(ongoingTaskId).getStatus() == TaskStatus.SUCCESS));

        Task task = client.getTaskById(taskId);
        assertTaskPrimary(task, EVENT, TaskStatus.SUCCESS, taskId, true, true);

        Task childTask = client.getTaskById(childTaskId);
        assertTask(childTask, EVENT + '2', true, true, taskId, taskId, TaskStatus.SUCCESS, false, EVENT);

        Task ongoingTask = client.getTaskById(ongoingTaskId);
        assertTask(ongoingTask, ongoingTaskName, true, true, taskId, taskId, TaskStatus.SUCCESS, false, EVENT);

        assertEquals(ctx, ongoingTask.getCtx().get(ctx));
        assertEquals(ctx1, ongoingTask.getCtx().get(ctx1));
        assertEquals(ctx2, ongoingTask.getCtx().get(ctx2));
        assertEquals(ctx3, ongoingTask.getCtx().get(ctx3));
        assertEquals(1, ongoingTask.getMetric().get(metric1).intValue());
        assertEquals(2, ongoingTask.getMetric().get(metric2).intValue());
        assertEquals(3, ongoingTask.getMetric().get(metric3).intValue());
        assertEquals(text1, ongoingTask.getText().get(text1));
        assertEquals(text2, ongoingTask.getText().get(text2));
        assertEquals(text3, ongoingTask.getText().get(text3));
        assertEquals(string1, ongoingTask.getString().get(string1));
        assertEquals(string2, ongoingTask.getString().get(string2));
        assertEquals(string3, ongoingTask.getString().get(string3));

        String[] split = ongoingTask.getLog().split("\n");
        assertEquals(3, split.length);
        assertTrue(split[0].matches(LOG_REGEX + log1));
        assertTrue(split[1].matches(LOG_REGEX + log2));
        assertTrue(split[2].matches(LOG_REGEX + log3));
    }

    public void testOutOfOrderTask() {
        String ctx1 = "ctx1";
        String ctx2 = "ctx2";
        String ctx3 = "ctx3";
        String metric1 = "metric1";
        String metric2 = "metric2";
        String metric3 = "metric3";
        String text1 = "text1";
        String text2 = "text2";
        String text3 = "text3";
        String string1 = "string1";
        String string2 = "string2";
        String string3 = "string3";
        String log1 = "log1";
        String log2 = "log2";
        String log3 = "log3";

        String ongoingTaskName = EVENT + '1';

        String taskId = Event.generateTaskId(ongoingTaskName);
        TimberLoggerAdvanced.logParams(taskId, LogParams.create().context(ctx2, ctx2).metric(metric2, 2).text(text2, text2).string(string2, string2).logInfo(log1));
        TimberLoggerAdvanced.success(taskId, LogParams.create().context(ctx3, ctx3).metric(metric3, 3).text(text3, text3).string(string3, string3).logInfo(log2));
        TimberLoggerAdvanced.start(taskId, ongoingTaskName, null, LogParams.create().context(ctx1, ctx1).metric(metric1, 1).text(text1, text1).string(string1, string1).logInfo(log3));

        Awaitility.await().atMost(90, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null)
                && (client.getTaskById(taskId).getStatus() == TaskStatus.SUCCESS));

        Task ongoingTask = client.getTaskById(taskId);
        assertTaskPrimary(ongoingTask, ongoingTaskName, TaskStatus.SUCCESS, taskId, true, true);

        assertEquals(ctx1, ongoingTask.getCtx().get(ctx1));
        assertEquals(ctx2, ongoingTask.getCtx().get(ctx2));
        assertEquals(ctx3, ongoingTask.getCtx().get(ctx3));
        assertEquals(1, ongoingTask.getMetric().get(metric1).intValue());
        assertEquals(2, ongoingTask.getMetric().get(metric2).intValue());
        assertEquals(3, ongoingTask.getMetric().get(metric3).intValue());
        assertEquals(text1, ongoingTask.getText().get(text1));
        assertEquals(text2, ongoingTask.getText().get(text2));
        assertEquals(text3, ongoingTask.getText().get(text3));
        assertEquals(string1, ongoingTask.getString().get(string1));
        assertEquals(string2, ongoingTask.getString().get(string2));
        assertEquals(string3, ongoingTask.getString().get(string3));

        String[] split = ongoingTask.getLog().split("\n");
        assertEquals(3, split.length);
        assertTrue(split[0].matches(LOG_REGEX + log1));
        assertTrue(split[1].matches(LOG_REGEX + log2));
        assertTrue(split[2].matches(LOG_REGEX + log3));
    }

    public void testOutOfOrderWithParentTask() {
        String ctx1 = "ctx1";
        String ctx2 = "ctx2";
        String ctx3 = "ctx3";
        String metric1 = "metric1";
        String metric2 = "metric2";
        String metric3 = "metric3";
        String text1 = "text1";
        String text2 = "text2";
        String text3 = "text3";
        String string1 = "string1";
        String string2 = "string2";
        String string3 = "string3";
        String log1 = "log1";
        String log2 = "log2";
        String log3 = "log3";

        String ongoingTaskName = EVENT + '1';

        String ongoingTaskId = Event.generateTaskId(ongoingTaskName);
        String taskId = testOutOfOrderWithParentTask1(ctx1, ctx2, ctx3, metric1, metric2, metric3, text1, text2, text3, string1, string2, string3, ongoingTaskName, ongoingTaskId, log1, log2, log3);

        Awaitility.await().atMost(90, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(ongoingTaskId) != null)
                && (client.getTaskById(ongoingTaskId).getStatus() == TaskStatus.SUCCESS));
        Awaitility.await().atMost(90, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null)
                && (client.getTaskById(taskId).getStatus() == TaskStatus.SUCCESS));

        Task ongoingTask = client.getTaskById(ongoingTaskId);
        assertTask(ongoingTask, ongoingTaskName, true, true, taskId, taskId, TaskStatus.SUCCESS, false, EVENT);

        Task task = client.getTaskById(taskId);
        assertTaskPrimary(task, EVENT, TaskStatus.SUCCESS, taskId, true, true);

        assertEquals(ctx1, ongoingTask.getCtx().get(ctx1));
        assertEquals(ctx2, ongoingTask.getCtx().get(ctx2));
        assertEquals(ctx3, ongoingTask.getCtx().get(ctx3));
        assertEquals(1, ongoingTask.getMetric().get(metric1).intValue());
        assertEquals(2, ongoingTask.getMetric().get(metric2).intValue());
        assertEquals(3, ongoingTask.getMetric().get(metric3).intValue());
        assertEquals(text1, ongoingTask.getText().get(text1));
        assertEquals(text2, ongoingTask.getText().get(text2));
        assertEquals(text3, ongoingTask.getText().get(text3));
        assertEquals(string1, ongoingTask.getString().get(string1));
        assertEquals(string2, ongoingTask.getString().get(string2));
        assertEquals(string3, ongoingTask.getString().get(string3));
        String[] split = ongoingTask.getLog().split("\n");
        assertEquals(3, split.length);
        assertTrue(split[0].matches(LOG_REGEX + log1));
        assertTrue(split[1].matches(LOG_REGEX + log2));
        assertTrue(split[2].matches(LOG_REGEX + log3));
    }

    @TimberLog(name = EVENT)
    private String testOutOfOrderWithParentTask1(String ctx1, String ctx2, String ctx3, String metric1, String metric2, String metric3, String text1, String text2, String text3, String string1, String string2, String string3, String ongoingTaskName, String taskId, String log1, String log2, String log3) {
        TimberLoggerAdvanced.logParams(taskId, LogParams.create().context(ctx2, ctx2).metric(metric2, 2).text(text2, text2).string(string2, string2).logInfo(log1));
        TimberLoggerAdvanced.success(taskId, LogParams.create().context(ctx3, ctx3).metric(metric3, 3).text(text3, text3).string(string3, string3).logInfo(log2));
        String currentTaskId = TimberLogger.getCurrentTaskId();
        TimberLoggerAdvanced.start(taskId, ongoingTaskName, currentTaskId, LogParams.create().context(ctx1, ctx1).metric(metric1, 1).text(text1, text1).string(string1, string1).logInfo(log3));
        return currentTaskId;
    }

    public void testOutOfOrderTaskError() {
        String ctx1 = "ctx1";
        String ctx2 = "ctx2";
        String ctx3 = "ctx3";
        String metric1 = "metric1";
        String metric2 = "metric2";
        String metric3 = "metric3";
        String text1 = "text1";
        String text2 = "text2";
        String text3 = "text3";
        String string1 = "string1";
        String string2 = "string2";
        String string3 = "string3";
        String log1 = "log1";
        String log2 = "log2";
        String log3 = "log3";

        String ongoingTaskName = EVENT + '1';

        String taskId = Event.generateTaskId(ongoingTaskName);
        String exception = "exception";
        TimberLoggerAdvanced.logParams(taskId, LogParams.create().context(ctx2, ctx2).metric(metric2, 2).text(text2, text2).string(string2, string2).logInfo(log1));
        TimberLoggerAdvanced.error(taskId, new Exception(exception), LogParams.create().context(ctx3, ctx3).metric(metric3, 3).text(text3, text3).string(string3, string3).logInfo(log2));
        TimberLoggerAdvanced.start(taskId, ongoingTaskName, null, LogParams.create().context(ctx1, ctx1).metric(metric1, 1).text(text1, text1).string(string1, string1).logInfo(log3));

        Awaitility.await().atMost(90, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null)
                && (client.getTaskById(taskId).getStatus() == TaskStatus.ERROR));

        Task ongoingTask = client.getTaskById(taskId);

        assertTaskPrimary(ongoingTask, ongoingTaskName, TaskStatus.ERROR, taskId, true, true);

        assertEquals(ctx1, ongoingTask.getCtx().get(ctx1));
        assertEquals(ctx2, ongoingTask.getCtx().get(ctx2));
        assertEquals(ctx3, ongoingTask.getCtx().get(ctx3));
        assertEquals(1, ongoingTask.getMetric().get(metric1).intValue());
        assertEquals(2, ongoingTask.getMetric().get(metric2).intValue());
        assertEquals(3, ongoingTask.getMetric().get(metric3).intValue());
        assertEquals(text1, ongoingTask.getText().get(text1));
        assertEquals(text2, ongoingTask.getText().get(text2));
        assertEquals(text3, ongoingTask.getText().get(text3));
        assertEquals(string1, ongoingTask.getString().get(string1));
        assertEquals(string2, ongoingTask.getString().get(string2));
        assertEquals(string3, ongoingTask.getString().get(string3));
        assertNotNull(ongoingTask.getText().get(exception));
        String[] split = ongoingTask.getLog().split("\n");
        assertEquals(3, split.length);
        assertTrue(split[0].matches(LOG_REGEX + log1));
        assertTrue(split[1].matches(LOG_REGEX + log2));
        assertTrue(split[2].matches(LOG_REGEX + log3));
    }

    public void testOutOfOrderTaskStartSuccessLog() {
        String ctx1 = "ctx1";
        String ctx2 = "ctx2";
        String ctx3 = "ctx3";
        String metric1 = "metric1";
        String metric2 = "metric2";
        String metric3 = "metric3";
        String text1 = "text1";
        String text2 = "text2";
        String text3 = "text3";
        String string1 = "string1";
        String string2 = "string2";
        String string3 = "string3";
        String log1 = "log1";
        String log2 = "log2";
        String log3 = "log3";

        String ongoingTaskName = EVENT + '1';

        String taskId = Event.generateTaskId(ongoingTaskName);
        TimberLoggerAdvanced.start(taskId, ongoingTaskName, null, LogParams.create().context(ctx1, ctx1).metric(metric1, 1).text(text1, text1).string(string1, string1).logInfo(log1));
        TimberLoggerAdvanced.success(taskId, LogParams.create().context(ctx3, ctx3).metric(metric3, 3).text(text3, text3).string(string3, string3).logInfo(log2));
        TimberLoggerAdvanced.logParams(taskId, LogParams.create().context(ctx2, ctx2).metric(metric2, 2).text(text2, text2).string(string2, string2).logInfo(log3));

        Awaitility.await().atMost(90, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null)
                && (client.getTaskById(taskId).getStatus() == TaskStatus.SUCCESS) && client.getTaskById(taskId).getCtx().get(ctx2) != null);

        Task ongoingTask = client.getTaskById(taskId);
        assertTaskPrimary(ongoingTask, ongoingTaskName, TaskStatus.SUCCESS, taskId, true, true);

        assertEquals(ctx1, ongoingTask.getCtx().get(ctx1));
        assertEquals(ctx2, ongoingTask.getCtx().get(ctx2));
        assertEquals(ctx3, ongoingTask.getCtx().get(ctx3));
        assertEquals(1, ongoingTask.getMetric().get(metric1).intValue());
        assertEquals(2, ongoingTask.getMetric().get(metric2).intValue());
        assertEquals(3, ongoingTask.getMetric().get(metric3).intValue());
        assertEquals(text1, ongoingTask.getText().get(text1));
        assertEquals(text2, ongoingTask.getText().get(text2));
        assertEquals(text3, ongoingTask.getText().get(text3));
        assertEquals(string1, ongoingTask.getString().get(string1));
        assertEquals(string2, ongoingTask.getString().get(string2));
        assertEquals(string3, ongoingTask.getString().get(string3));
        String[] split = ongoingTask.getLog().split("\n");
        assertEquals(3, split.length);
        assertTrue(split[0].matches(LOG_REGEX + log1));
        assertTrue(split[1].matches(LOG_REGEX + log2));
        assertTrue(split[2].matches(LOG_REGEX + log3));
    }

    public void testOutOfOrderTaskLogStartSuccess() {
        String ctx1 = "ctx1";
        String ctx2 = "ctx2";
        String ctx3 = "ctx3";
        String metric1 = "metric1";
        String metric2 = "metric2";
        String metric3 = "metric3";
        String text1 = "text1";
        String text2 = "text2";
        String text3 = "text3";
        String string1 = "string1";
        String string2 = "string2";
        String string3 = "string3";
        String log1 = "log1";
        String log2 = "log2";
        String log3 = "log3";

        String ongoingTaskName = EVENT + '1';

        String taskId = Event.generateTaskId(ongoingTaskName);
        TimberLoggerAdvanced.logParams(taskId, LogParams.create().context(ctx2, ctx2).metric(metric2, 2).text(text2, text2).string(string2, string2).logInfo(log1));
        TimberLoggerAdvanced.start(taskId, ongoingTaskName, null, LogParams.create().context(ctx1, ctx1).metric(metric1, 1).text(text1, text1).string(string1, string1).logInfo(log2));
        TimberLoggerAdvanced.success(taskId, LogParams.create().context(ctx3, ctx3).metric(metric3, 3).text(text3, text3).string(string3, string3).logInfo(log3));

        Awaitility.await().atMost(90, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null)
                && (client.getTaskById(taskId).getStatus() == TaskStatus.SUCCESS));

        Task ongoingTask = client.getTaskById(taskId);
        assertTaskPrimary(ongoingTask, ongoingTaskName, TaskStatus.SUCCESS, taskId, true, true);

        assertEquals(ctx1, ongoingTask.getCtx().get(ctx1));
        assertEquals(ctx2, ongoingTask.getCtx().get(ctx2));
        assertEquals(ctx3, ongoingTask.getCtx().get(ctx3));
        assertEquals(1, ongoingTask.getMetric().get(metric1).intValue());
        assertEquals(2, ongoingTask.getMetric().get(metric2).intValue());
        assertEquals(3, ongoingTask.getMetric().get(metric3).intValue());
        assertEquals(text1, ongoingTask.getText().get(text1));
        assertEquals(text2, ongoingTask.getText().get(text2));
        assertEquals(text3, ongoingTask.getText().get(text3));
        assertEquals(string1, ongoingTask.getString().get(string1));
        assertEquals(string2, ongoingTask.getString().get(string2));
        assertEquals(string3, ongoingTask.getString().get(string3));
        String[] split = ongoingTask.getLog().split("\n");
        assertEquals(3, split.length);
        assertTrue(split[0].matches(LOG_REGEX + log1));
        assertTrue(split[1].matches(LOG_REGEX + log2));
        assertTrue(split[2].matches(LOG_REGEX + log3));
    }

    public void testOutOfOrderTaskSuccessLogStart() {
        String ctx1 = "ctx1";
        String ctx2 = "ctx2";
        String ctx3 = "ctx3";
        String metric1 = "metric1";
        String metric2 = "metric2";
        String metric3 = "metric3";
        String text1 = "text1";
        String text2 = "text2";
        String text3 = "text3";
        String string1 = "string1";
        String string2 = "string2";
        String string3 = "string3";
        String log1 = "log1";
        String log2 = "log2";
        String log3 = "log3";

        String ongoingTaskName = EVENT + '1';

        String taskId = Event.generateTaskId(ongoingTaskName);
        TimberLoggerAdvanced.success(taskId, LogParams.create().context(ctx3, ctx3).metric(metric3, 3).text(text3, text3).string(string3, string3).logInfo(log1));
        TimberLoggerAdvanced.logParams(taskId, LogParams.create().context(ctx2, ctx2).metric(metric2, 2).text(text2, text2).string(string2, string2).logInfo(log2));
        TimberLoggerAdvanced.start(taskId, ongoingTaskName, null, LogParams.create().context(ctx1, ctx1).metric(metric1, 1).text(text1, text1).string(string1, string1).logInfo(log3));

        Awaitility.await().atMost(90, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null)
                && (client.getTaskById(taskId).getStatus() == TaskStatus.SUCCESS));

        Task ongoingTask = client.getTaskById(taskId);
        assertTaskPrimary(ongoingTask, ongoingTaskName, TaskStatus.SUCCESS, taskId, true, true);

        assertEquals(ctx1, ongoingTask.getCtx().get(ctx1));
        assertEquals(ctx2, ongoingTask.getCtx().get(ctx2));
        assertEquals(ctx3, ongoingTask.getCtx().get(ctx3));
        assertEquals(1, ongoingTask.getMetric().get(metric1).intValue());
        assertEquals(2, ongoingTask.getMetric().get(metric2).intValue());
        assertEquals(3, ongoingTask.getMetric().get(metric3).intValue());
        assertEquals(text1, ongoingTask.getText().get(text1));
        assertEquals(text2, ongoingTask.getText().get(text2));
        assertEquals(text3, ongoingTask.getText().get(text3));
        assertEquals(string1, ongoingTask.getString().get(string1));
        assertEquals(string2, ongoingTask.getString().get(string2));
        assertEquals(string3, ongoingTask.getString().get(string3));
        String[] split = ongoingTask.getLog().split("\n");
        assertEquals(3, split.length);
        assertTrue(split[0].matches(LOG_REGEX + log1));
        assertTrue(split[1].matches(LOG_REGEX + log2));
        assertTrue(split[2].matches(LOG_REGEX + log3));
    }

    public void testOutOfOrderTaskSuccessStartLog() {
        String ctx1 = "ctx1";
        String ctx2 = "ctx2";
        String ctx3 = "ctx3";
        String metric1 = "metric1";
        String metric2 = "metric2";
        String metric3 = "metric3";
        String text1 = "text1";
        String text2 = "text2";
        String text3 = "text3";
        String string1 = "string1";
        String string2 = "string2";
        String string3 = "string3";
        String log1 = "log1";
        String log2 = "log2";
        String log3 = "log3";

        String ongoingTaskName = EVENT + '1';

        String taskId = Event.generateTaskId(ongoingTaskName);
        TimberLoggerAdvanced.success(taskId, LogParams.create().context(ctx3, ctx3).metric(metric3, 3).text(text3, text3).string(string3, string3).logInfo(log1));
        TimberLoggerAdvanced.start(taskId, ongoingTaskName, null, LogParams.create().context(ctx1, ctx1).metric(metric1, 1).text(text1, text1).string(string1, string1).logInfo(log2));
        TimberLoggerAdvanced.logParams(taskId, LogParams.create().context(ctx2, ctx2).metric(metric2, 2).text(text2, text2).string(string2, string2).logInfo(log3));

        Awaitility.await().atMost(90, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null)
                && (client.getTaskById(taskId).getStatus() == TaskStatus.SUCCESS) && client.getTaskById(taskId).getCtx().get(ctx2) != null);

        Task ongoingTask = client.getTaskById(taskId);
        assertTaskPrimary(ongoingTask, ongoingTaskName, TaskStatus.SUCCESS, taskId, true, true);

        assertEquals(ctx1, ongoingTask.getCtx().get(ctx1));
        assertEquals(ctx2, ongoingTask.getCtx().get(ctx2));
        assertEquals(ctx3, ongoingTask.getCtx().get(ctx3));
        assertEquals(1, ongoingTask.getMetric().get(metric1).intValue());
        assertEquals(2, ongoingTask.getMetric().get(metric2).intValue());
        assertEquals(3, ongoingTask.getMetric().get(metric3).intValue());
        assertEquals(text1, ongoingTask.getText().get(text1));
        assertEquals(text2, ongoingTask.getText().get(text2));
        assertEquals(text3, ongoingTask.getText().get(text3));
        assertEquals(string1, ongoingTask.getString().get(string1));
        assertEquals(string2, ongoingTask.getString().get(string2));
        assertEquals(string3, ongoingTask.getString().get(string3));
        String[] split = ongoingTask.getLog().split("\n");
        assertEquals(3, split.length);
        assertTrue(split[0].matches(LOG_REGEX + log1));
        assertTrue(split[1].matches(LOG_REGEX + log2));
        assertTrue(split[2].matches(LOG_REGEX + log3));
    }

    public void testOutOfOrderTaskSuccessLogNoStart() {
        String ctx2 = "ctx2";
        String ctx3 = "ctx3";
        String metric2 = "metric2";
        String metric3 = "metric3";
        String text2 = "text2";
        String text3 = "text3";
        String string2 = "string2";
        String string3 = "string3";
        String log1 = "log1";
        String log2 = "log2";

        String ongoingTaskName = EVENT + '1';

        String taskId = Event.generateTaskId(ongoingTaskName);
        TimberLoggerAdvanced.success(taskId, LogParams.create().context(ctx3, ctx3).metric(metric3, 3).text(text3, text3).string(string3, string3).logInfo(log1));
        TimberLoggerAdvanced.logParams(taskId, LogParams.create().context(ctx2, ctx2).metric(metric2, 2).text(text2, text2).string(string2, string2).logInfo(log2));

        Awaitility.await().atMost(90, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null)
                && (client.getTaskById(taskId).getCtx().get(ctx2) != null));

        Task ongoingTask = client.getTaskById(taskId);
        assertTaskCorrupted(ongoingTask, ongoingTaskName, TaskStatus.CORRUPTED_SUCCESS, true);

        assertEquals(ctx2, ongoingTask.getCtx().get(ctx2));
        assertEquals(ctx3, ongoingTask.getCtx().get(ctx3));
        assertEquals(2, ongoingTask.getMetric().get(metric2).intValue());
        assertEquals(3, ongoingTask.getMetric().get(metric3).intValue());
        assertEquals(text2, ongoingTask.getText().get(text2));
        assertEquals(text3, ongoingTask.getText().get(text3));
        assertEquals(string2, ongoingTask.getString().get(string2));
        assertEquals(string3, ongoingTask.getString().get(string3));
        String[] split = ongoingTask.getLog().split("\n");
        assertEquals(2, split.length);
        assertTrue(split[0].matches(LOG_REGEX + log1));
        assertTrue(split[1].matches(LOG_REGEX + log2));
    }

    public void testOutOfOrderTaskErrorLogNoStart() {
        String ctx2 = "ctx2";
        String ctx3 = "ctx3";
        String metric2 = "metric2";
        String metric3 = "metric3";
        String text2 = "text2";
        String text3 = "text3";
        String string2 = "string2";
        String string3 = "string3";
        String log1 = "log1";
        String log2 = "log2";

        String ongoingTaskName = EVENT + '1';

        String taskId = Event.generateTaskId(ongoingTaskName);
        TimberLoggerAdvanced.error(taskId, new Exception("exception"), LogParams.create().context(ctx3, ctx3).metric(metric3, 3).text(text3, text3).string(string3, string3).logInfo(log1));
        TimberLoggerAdvanced.logParams(taskId, LogParams.create().context(ctx2, ctx2).metric(metric2, 2).text(text2, text2).string(string2, string2).logInfo(log2));

        Awaitility.await().atMost(90, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null)
                && (client.getTaskById(taskId).getCtx().get(ctx2) != null));

        Task ongoingTask = client.getTaskById(taskId);
        assertTaskCorrupted(ongoingTask, ongoingTaskName, TaskStatus.CORRUPTED_ERROR, true);

        assertEquals(ctx2, ongoingTask.getCtx().get(ctx2));
        assertEquals(ctx3, ongoingTask.getCtx().get(ctx3));
        assertEquals(2, ongoingTask.getMetric().get(metric2).intValue());
        assertEquals(3, ongoingTask.getMetric().get(metric3).intValue());
        assertEquals(text2, ongoingTask.getText().get(text2));
        assertEquals(text3, ongoingTask.getText().get(text3));
        assertEquals(string2, ongoingTask.getString().get(string2));
        assertEquals(string3, ongoingTask.getString().get(string3));
        assertNotNull(ongoingTask.getText().get("exception"));
        String[] split = ongoingTask.getLog().split("\n");
        assertEquals(2, split.length);
        assertTrue(split[0].matches(LOG_REGEX + log1));
        assertTrue(split[1].matches(LOG_REGEX + log2));
    }

    public void testOutOfOrderTaskLogSuccessNoStart() {
        String ctx2 = "ctx2";
        String ctx3 = "ctx3";
        String metric2 = "metric2";
        String metric3 = "metric3";
        String text2 = "text2";
        String text3 = "text3";
        String string2 = "string2";
        String string3 = "string3";
        String log1 = "log1";
        String log2 = "log2";

        String ongoingTaskName = EVENT + '1';

        String taskId = Event.generateTaskId(ongoingTaskName);
        TimberLoggerAdvanced.logParams(taskId, LogParams.create().context(ctx2, ctx2).metric(metric2, 2).text(text2, text2).string(string2, string2).logInfo(log1));
        TimberLoggerAdvanced.success(taskId, LogParams.create().context(ctx3, ctx3).metric(metric3, 3).text(text3, text3).string(string3, string3).logInfo(log2));

        Awaitility.await().atMost(90, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null)
                && (client.getTaskById(taskId).getCtx().get(ctx3) != null));

        Task ongoingTask = client.getTaskById(taskId);
        assertTaskCorrupted(ongoingTask, ongoingTaskName, TaskStatus.CORRUPTED_SUCCESS, true);

        assertEquals(ongoingTaskName, ongoingTask.getName());
        assertEquals(ctx2, ongoingTask.getCtx().get(ctx2));
        assertEquals(ctx3, ongoingTask.getCtx().get(ctx3));
        assertEquals(2, ongoingTask.getMetric().get(metric2).intValue());
        assertEquals(3, ongoingTask.getMetric().get(metric3).intValue());
        assertEquals(text2, ongoingTask.getText().get(text2));
        assertEquals(text3, ongoingTask.getText().get(text3));
        assertEquals(string2, ongoingTask.getString().get(string2));
        assertEquals(string3, ongoingTask.getString().get(string3));
        String[] split = ongoingTask.getLog().split("\n");
        assertEquals(2, split.length);
        assertTrue(split[0].matches(LOG_REGEX + log1));
        assertTrue(split[1].matches(LOG_REGEX + log2));
    }

    public void testOutOfOrderTaskStartLogNoSuccess() {
        String ctx1 = "ctx1";
        String ctx2 = "ctx2";
        String metric1 = "metric1";
        String metric2 = "metric2";
        String text1 = "text1";
        String text2 = "text2";
        String string1 = "string1";
        String string2 = "string2";
        String log1 = "log1";
        String log2 = "log2";

        String ongoingTaskName = EVENT + '1';

        String taskId = Event.generateTaskId(ongoingTaskName);
        TimberLoggerAdvanced.start(taskId, ongoingTaskName, null, LogParams.create().context(ctx1, ctx1).metric(metric1, 1).text(text1, text1).string(string1, string1).logInfo(log1));
        TimberLoggerAdvanced.logParams(taskId, LogParams.create().context(ctx2, ctx2).metric(metric2, 2).text(text2, text2).string(string2, string2).logInfo(log2));

        Awaitility.await().atMost(90, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null)
                && (client.getTaskById(taskId).getCtx().get(ctx2) != null));

        Task ongoingTask = client.getTaskById(taskId);
        assertTaskPrimary(ongoingTask, ongoingTaskName, TaskStatus.UNTERMINATED, taskId, false, false);

        assertEquals(ctx1, ongoingTask.getCtx().get(ctx1));
        assertEquals(ctx2, ongoingTask.getCtx().get(ctx2));
        assertEquals(1, ongoingTask.getMetric().get(metric1).intValue());
        assertEquals(2, ongoingTask.getMetric().get(metric2).intValue());
        assertEquals(text1, ongoingTask.getText().get(text1));
        assertEquals(text2, ongoingTask.getText().get(text2));
        assertEquals(string1, ongoingTask.getString().get(string1));
        assertEquals(string2, ongoingTask.getString().get(string2));
        String[] split = ongoingTask.getLog().split("\n");
        assertEquals(2, split.length);
        assertTrue(split[0].matches(LOG_REGEX + log1));
        assertTrue(split[1].matches(LOG_REGEX + log2));
    }

    public void testOutOfOrderTaskLogStartNoSuccess() {
        String ctx1 = "ctx1";
        String ctx2 = "ctx2";
        String metric1 = "metric1";
        String metric2 = "metric2";
        String text1 = "text1";
        String text2 = "text2";
        String string1 = "string1";
        String string2 = "string2";
        String log1 = "log1";
        String log2 = "log2";

        String ongoingTaskName = EVENT + '1';

        String taskId = Event.generateTaskId(ongoingTaskName);
        TimberLoggerAdvanced.logParams(taskId, LogParams.create().context(ctx2, ctx2).metric(metric2, 2).text(text2, text2).string(string2, string2).logInfo(log1));
        TimberLoggerAdvanced.start(taskId, ongoingTaskName, null, LogParams.create().context(ctx1, ctx1).metric(metric1, 1).text(text1, text1).string(string1, string1).logInfo(log2));

        Awaitility.await().atMost(90, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null)
                && (client.getTaskById(taskId).getCtx().get(ctx1) != null));

        Task ongoingTask = client.getTaskById(taskId);
        assertTaskPrimary(ongoingTask, ongoingTaskName, TaskStatus.UNTERMINATED, taskId, false, false);

        assertEquals(ctx1, ongoingTask.getCtx().get(ctx1));
        assertEquals(ctx2, ongoingTask.getCtx().get(ctx2));
        assertEquals(1, ongoingTask.getMetric().get(metric1).intValue());
        assertEquals(2, ongoingTask.getMetric().get(metric2).intValue());
        assertEquals(text1, ongoingTask.getText().get(text1));
        assertEquals(text2, ongoingTask.getText().get(text2));
        assertEquals(string1, ongoingTask.getString().get(string1));
        assertEquals(string2, ongoingTask.getString().get(string2));
        String[] split = ongoingTask.getLog().split("\n");
        assertEquals(2, split.length);
        assertTrue(split[0].matches(LOG_REGEX + log1));
        assertTrue(split[1].matches(LOG_REGEX + log2));
    }

    public void testOnlyLog() {
        String ctx2 = "ctx2";
        String metric2 = "metric2";
        String text2 = "text2";
        String string2 = "string2";
        String log1 = "log1";
        String log2 = "log2";
        String log3 = "log3";

        String ongoingTaskName = EVENT + '1';

        String taskId = Event.generateTaskId(ongoingTaskName);
        TimberLoggerAdvanced.logParams(taskId, LogParams.create().context(ctx2, ctx2).metric(metric2, 2).text(text2, text2).string(string2, string2).logInfo(log1).logInfo(log2).logInfo(log3));

        Awaitility.await().atMost(90, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null));

        Task ongoingTask = client.getTaskById(taskId);
        assertTaskCorrupted(ongoingTask, ongoingTaskName, TaskStatus.CORRUPTED, false);

        assertEquals(ctx2, ongoingTask.getCtx().get(ctx2));
        assertEquals(2, ongoingTask.getMetric().get(metric2).intValue());
        assertEquals(text2, ongoingTask.getText().get(text2));
        assertEquals(string2, ongoingTask.getString().get(string2));
        String[] split = ongoingTask.getLog().split("\n");
        assertEquals(3, split.length);
        assertTrue(split[0].matches(LOG_REGEX + log1));
        assertTrue(split[1].matches(LOG_REGEX + log2));
        assertTrue(split[2].matches(LOG_REGEX + log3));
    }

    @TimberLog(name = EVENT)
    private void testOngoingTask1(String[] taskId1Arr, String[] taskId2Arr, String[] ongoingTaskIdArr, String ctx, String ctx1, String ctx2, String ctx3, String metric1, String metric2, String metric3, String text1, String text2, String text3, String string1, String string2, String string3, String log1, String log2, String log3, String ongoingTaskName) {
        TimberLogger.logParams(LogParams.create().context(ctx, ctx));
        taskId1Arr[0] = TimberLogger.getCurrentTaskId();

        ongoingTaskIdArr[0] = TimberLoggerAdvanced.start(ongoingTaskName, taskId1Arr[0], LogParams.create().context(ctx1, ctx1).text(text1, text1).metric(metric1, 1).string(string1, string1).logInfo(log1));

        taskId2Arr[0] = testOngoingTask2();

        new Thread(() -> {
            TimberLoggerAdvanced.logParams(ongoingTaskIdArr[0], LogParams.create().context(ctx2, ctx2).metric(metric2, 2).text(text2, text2).string(string2, string2).logInfo(log2));
            TimberLoggerAdvanced.success(ongoingTaskIdArr[0], LogParams.create().context(ctx3, ctx3).metric(metric3, 3).text(text3, text3).string(string3, string3).logInfo(log3));
        }).run();
    }

    @TimberLog(name = EVENT + '2')
    private String testOngoingTask2() {
        return TimberLogger.getCurrentTaskId();
    }

    public void testOngoingPrimaryTask() {
        final String[] taskId1Arr = new String[1];

        String ctx1 = "ctx1";

        String ongoingTaskName = EVENT + '1';

        String parentTaskId = TimberLoggerAdvanced.start(ongoingTaskName, LogParams.create().context(ctx1, ctx1));

        new Thread(() -> {
            try(TimberLogContext ignored = new TimberLogContext(parentTaskId)) {
                taskId1Arr[0] = testOngoingPrimaryTask2();
            } catch (Exception e) {
                fail(e.getMessage());
            }
        }).run();

        TimberLoggerAdvanced.success(parentTaskId, LogParams.create());

        Awaitility.await().atMost(90, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(parentTaskId) != null)
                && (client.getTaskById(parentTaskId).getStatus() == TaskStatus.SUCCESS));
        Awaitility.await().atMost(90, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId1Arr[0]) != null)
                && (client.getTaskById(taskId1Arr[0]).getStatus() == TaskStatus.SUCCESS));

        Task parentTask = client.getTaskById(parentTaskId);
        assertTaskPrimary(parentTask, ongoingTaskName, TaskStatus.SUCCESS, parentTaskId, true, true);
        assertEquals(ctx1, parentTask.getCtx().get(ctx1));

        Task childTask = client.getTaskById(taskId1Arr[0]);
        assertTask(childTask, EVENT + '2', true, true, parentTaskId, parentTaskId, TaskStatus.SUCCESS, false, ongoingTaskName);
        assertEquals(ctx1, childTask.getCtx().get(ctx1));
    }

    @TimberLog(name = EVENT + '2')
    private String testOngoingPrimaryTask2() {
        return TimberLogger.getCurrentTaskId();

    }

    public void testOngoingTaskWithContext() {
        final String[] taskIdArr = new String[1];


        String ongoingTaskName = EVENT + '1';

        String ongoingTaskId = TimberLoggerAdvanced.start(ongoingTaskName);

        new Thread(() -> {
            try (TimberLogContext ignored = new TimberLogContext(ongoingTaskId)) {
                taskIdArr[0] = testOngoingTaskWithContext2();
            } catch (Exception ignored) {

            }
        }).run();

        TimberLoggerAdvanced.success(ongoingTaskId, LogParams.create());

        String childTaskId = taskIdArr[0];

        Awaitility.await().atMost(90, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(ongoingTaskId) != null)
                && (client.getTaskById(ongoingTaskId).getStatus() == TaskStatus.SUCCESS));
        Awaitility.await().atMost(90, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(childTaskId) != null)
                && (client.getTaskById(childTaskId).getStatus() == TaskStatus.SUCCESS));

        Task childTask = client.getTaskById(childTaskId);
        assertTask(childTask, EVENT + '2', true, true, ongoingTaskId, ongoingTaskId, TaskStatus.SUCCESS, false, ongoingTaskName);

        Task ongoingTask = client.getTaskById(ongoingTaskId);
        assertTaskPrimary(ongoingTask,  ongoingTaskName, TaskStatus.SUCCESS, ongoingTaskId, true, true);
    }

    @TimberLog(name = EVENT + '2')
    private String testOngoingTaskWithContext2() {
        return TimberLogger.getCurrentTaskId();
    }

    public void testOngoingTaskWithNullContext() {
        String taskId = null;

        try (TimberLogContext ignored = new TimberLogContext(null)) {
            taskId = testOngoingTaskWithNullContext2();
        } catch (Exception ignored) {
        }

        String finalTaskId = taskId;
        Awaitility.await().atMost(90, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(finalTaskId) != null)
                && (client.getTaskById(finalTaskId).getStatus() == TaskStatus.SUCCESS));

        Task childTask = client.getTaskById(taskId);
        assertTaskPrimary(childTask, EVENT + '2', TaskStatus.SUCCESS, taskId, true, true);
    }

    @TimberLog(name = EVENT + '2')
    private String testOngoingTaskWithNullContext2() {
        return TimberLogger.getCurrentTaskId();
    }
}

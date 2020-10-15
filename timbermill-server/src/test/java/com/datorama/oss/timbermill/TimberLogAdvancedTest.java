package com.datorama.oss.timbermill;

import java.time.ZonedDateTime;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.datorama.oss.timbermill.annotation.TimberLogTask;
import com.datorama.oss.timbermill.unit.LogParams;
import com.datorama.oss.timbermill.unit.StartEvent;
import com.datorama.oss.timbermill.unit.Task;
import com.datorama.oss.timbermill.unit.TaskStatus;

import static com.datorama.oss.timbermill.TimberLogTest.*;
import static com.datorama.oss.timbermill.common.Constants.CORRUPTED_REASON;
import static com.datorama.oss.timbermill.unit.Event.generateTaskId;
import static com.datorama.oss.timbermill.unit.SuccessEvent.ALREADY_CLOSED_DIFFERENT_CLOSE_STATUS;
import static com.datorama.oss.timbermill.unit.SuccessEvent.ALREADY_CLOSED_DIFFERENT_CLOSE_TIME;
import static org.junit.Assert.*;

public class TimberLogAdvancedTest {

    private static ElasticsearchClient client;

    @BeforeClass
    public static void setUp() {
        String elasticUrl = System.getenv("ELASTICSEARCH_URL");
        if (StringUtils.isEmpty(elasticUrl)){
            elasticUrl = "http://localhost:9200";
        }

        client = new ElasticsearchClientForTests(elasticUrl, null
        );
    }

    @AfterClass
    public static void kill() {
        TimberLogger.exit();
    }

    private void waitForValueInContext(String key, String taskId) {
        Callable<Boolean> callable = () -> client.getTaskById(taskId).getCtx().get(key) != null;
        TimberLogTest.waitForCallable(callable);
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

        TimberLogTest.waitForTask(taskId, TaskStatus.SUCCESS);
        TimberLogTest.waitForTask(childTaskId, TaskStatus.SUCCESS);
        TimberLogTest.waitForTask(ongoingTaskId, TaskStatus.SUCCESS);

        Task task = client.getTaskById(taskId);
        assertTaskPrimary(task, EVENT, TaskStatus.SUCCESS, taskId, true, true);

        Task childTask = client.getTaskById(childTaskId);
        assertTask(childTask, EVENT + '2', true, true, taskId, taskId, TaskStatus.SUCCESS, EVENT);

        Task ongoingTask = client.getTaskById(ongoingTaskId);
        assertTask(ongoingTask, ongoingTaskName, true, true, taskId, taskId, TaskStatus.SUCCESS, EVENT);

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

        String taskId = generateTaskId(ongoingTaskName);
        TimberLoggerAdvanced.logParams(taskId, LogParams.create().context(ctx2, ctx2).metric(metric2, 2).text(text2, text2).string(string2, string2).logInfo(log1));
        TimberLoggerAdvanced.success(taskId, LogParams.create().context(ctx3, ctx3).metric(metric3, 3).text(text3, text3).string(string3, string3).logInfo(log2));
        TimberLoggerAdvanced.start(taskId, ongoingTaskName, null, LogParams.create().context(ctx1, ctx1).metric(metric1, 1).text(text1, text1).string(string1, string1).logInfo(log3));

        TimberLogTest.waitForTask(taskId, TaskStatus.SUCCESS);

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

        String ongoingTaskId = generateTaskId(ongoingTaskName);
        String taskId = testOutOfOrderWithParentTask1(ctx1, ctx2, ctx3, metric1, metric2, metric3, text1, text2, text3, string1, string2, string3, ongoingTaskName, ongoingTaskId, log1, log2, log3);

        TimberLogTest.waitForTask(ongoingTaskId, TaskStatus.SUCCESS);
        TimberLogTest.waitForTask(taskId, TaskStatus.SUCCESS);

        Task ongoingTask = client.getTaskById(ongoingTaskId);
        assertTask(ongoingTask, ongoingTaskName, true, true, taskId, taskId, TaskStatus.SUCCESS, EVENT);

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
    }

    @TimberLogTask(name = EVENT)
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

        String taskId = generateTaskId(ongoingTaskName);
        String exception = "exception";
        TimberLoggerAdvanced.logParams(taskId, LogParams.create().context(ctx2, ctx2).metric(metric2, 2).text(text2, text2).string(string2, string2).logInfo(log1));
        TimberLoggerAdvanced.error(taskId, new Exception(exception), LogParams.create().context(ctx3, ctx3).metric(metric3, 3).text(text3, text3).string(string3, string3).logInfo(log2));
        TimberLoggerAdvanced.start(taskId, ongoingTaskName, null, LogParams.create().context(ctx1, ctx1).metric(metric1, 1).text(text1, text1).string(string1, string1).logInfo(log3));

        TimberLogTest.waitForTask(taskId, TaskStatus.ERROR);

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

        String taskId = generateTaskId(ongoingTaskName);
        TimberLoggerAdvanced.start(taskId, ongoingTaskName, null, LogParams.create().context(ctx1, ctx1).metric(metric1, 1).text(text1, text1).string(string1, string1).logInfo(log1));
        TimberLoggerAdvanced.success(taskId, LogParams.create().context(ctx3, ctx3).metric(metric3, 3).text(text3, text3).string(string3, string3).logInfo(log2));
        TimberLoggerAdvanced.logParams(taskId, LogParams.create().context(ctx2, ctx2).metric(metric2, 2).text(text2, text2).string(string2, string2).logInfo(log3));

        TimberLogTest.waitForTask(taskId, TaskStatus.SUCCESS);
        waitForValueInContext(ctx2, taskId);

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

        String taskId = generateTaskId(ongoingTaskName);
        TimberLoggerAdvanced.logParams(taskId, LogParams.create().context(ctx2, ctx2).metric(metric2, 2).text(text2, text2).string(string2, string2).logInfo(log1));
        TimberLoggerAdvanced.start(taskId, ongoingTaskName, null, LogParams.create().context(ctx1, ctx1).metric(metric1, 1).text(text1, text1).string(string1, string1).logInfo(log2));
        TimberLoggerAdvanced.success(taskId, LogParams.create().context(ctx3, ctx3).metric(metric3, 3).text(text3, text3).string(string3, string3).logInfo(log3));

        TimberLogTest.waitForTask(taskId, TaskStatus.SUCCESS);

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

        String taskId = generateTaskId(ongoingTaskName);
        TimberLoggerAdvanced.success(taskId, LogParams.create().context(ctx3, ctx3).metric(metric3, 3).text(text3, text3).string(string3, string3).logInfo(log1));
        TimberLoggerAdvanced.logParams(taskId, LogParams.create().context(ctx2, ctx2).metric(metric2, 2).text(text2, text2).string(string2, string2).logInfo(log2));
        TimberLoggerAdvanced.start(taskId, ongoingTaskName, null, LogParams.create().context(ctx1, ctx1).metric(metric1, 1).text(text1, text1).string(string1, string1).logInfo(log3));

        TimberLogTest.waitForTask(taskId, TaskStatus.SUCCESS);

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

        String taskId = generateTaskId(ongoingTaskName);
        TimberLoggerAdvanced.success(taskId, LogParams.create().context(ctx3, ctx3).metric(metric3, 3).text(text3, text3).string(string3, string3).logInfo(log1));
        TimberLoggerAdvanced.start(taskId, ongoingTaskName, null, LogParams.create().context(ctx1, ctx1).metric(metric1, 1).text(text1, text1).string(string1, string1).logInfo(log2));
        TimberLoggerAdvanced.logParams(taskId, LogParams.create().context(ctx2, ctx2).metric(metric2, 2).text(text2, text2).string(string2, string2).logInfo(log3));

        TimberLogTest.waitForTask(taskId, TaskStatus.SUCCESS);
        waitForValueInContext(ctx2, taskId);

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

        String taskId = generateTaskId(ongoingTaskName);
        TimberLoggerAdvanced.success(taskId, LogParams.create().context(ctx3, ctx3).metric(metric3, 3).text(text3, text3).string(string3, string3).logInfo(log1));
        TimberLoggerAdvanced.logParams(taskId, LogParams.create().context(ctx2, ctx2).metric(metric2, 2).text(text2, text2).string(string2, string2).logInfo(log2));

        TimberLogTest.waitForTask(taskId, TaskStatus.PARTIAL_SUCCESS);
        waitForValueInContext(ctx2, taskId);

        Task ongoingTask = client.getTaskById(taskId);
        assertTaskCorrupted(ongoingTask, ongoingTaskName, TaskStatus.PARTIAL_SUCCESS, true);

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

        String taskId = generateTaskId(ongoingTaskName);
        TimberLoggerAdvanced.error(taskId, new Exception("exception"), LogParams.create().context(ctx3, ctx3).metric(metric3, 3).text(text3, text3).string(string3, string3).logInfo(log1));
        TimberLoggerAdvanced.logParams(taskId, LogParams.create().context(ctx2, ctx2).metric(metric2, 2).text(text2, text2).string(string2, string2).logInfo(log2));

        TimberLogTest.waitForTask(taskId, TaskStatus.PARTIAL_ERROR);
        waitForValueInContext(ctx2, taskId);

        Task ongoingTask = client.getTaskById(taskId);
        assertTaskCorrupted(ongoingTask, ongoingTaskName, TaskStatus.PARTIAL_ERROR, true);

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

        String taskId = generateTaskId(ongoingTaskName);
        TimberLoggerAdvanced.logParams(taskId, LogParams.create().context(ctx2, ctx2).metric(metric2, 2).text(text2, text2).string(string2, string2).logInfo(log1));
        TimberLoggerAdvanced.success(taskId, LogParams.create().context(ctx3, ctx3).metric(metric3, 3).text(text3, text3).string(string3, string3).logInfo(log2));


        TimberLogTest.waitForTask(taskId, TaskStatus.PARTIAL_SUCCESS);
        waitForValueInContext(ctx3, taskId);

        Task ongoingTask = client.getTaskById(taskId);
        assertTaskCorrupted(ongoingTask, ongoingTaskName, TaskStatus.PARTIAL_SUCCESS, true);

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

        String taskId = generateTaskId(ongoingTaskName);
        TimberLoggerAdvanced.start(taskId, ongoingTaskName, null, LogParams.create().context(ctx1, ctx1).metric(metric1, 1).text(text1, text1).string(string1, string1).logInfo(log1));
        TimberLoggerAdvanced.logParams(taskId, LogParams.create().context(ctx2, ctx2).metric(metric2, 2).text(text2, text2).string(string2, string2).logInfo(log2));

        TimberLogTest.waitForTask(taskId, TaskStatus.UNTERMINATED);
        waitForValueInContext(ctx2, taskId);

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

        String taskId = generateTaskId(ongoingTaskName);
        TimberLoggerAdvanced.logParams(taskId, LogParams.create().context(ctx2, ctx2).metric(metric2, 2).text(text2, text2).string(string2, string2).logInfo(log1));
        TimberLoggerAdvanced.start(taskId, ongoingTaskName, null, LogParams.create().context(ctx1, ctx1).metric(metric1, 1).text(text1, text1).string(string1, string1).logInfo(log2));

        TimberLogTest.waitForTask(taskId, TaskStatus.UNTERMINATED);
        waitForValueInContext(ctx1, taskId);

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

        String taskId = generateTaskId(ongoingTaskName);
        TimberLoggerAdvanced.logParams(taskId, LogParams.create().context(ctx2, ctx2).metric(metric2, 2).text(text2, text2).string(string2, string2).logInfo(log1).logInfo(log2).logInfo(log3));

        TimberLogTest.waitForTask(taskId, TaskStatus.PARTIAL_INFO_ONLY);

        Task ongoingTask = client.getTaskById(taskId);
        assertTaskCorrupted(ongoingTask, ongoingTaskName, TaskStatus.PARTIAL_INFO_ONLY, false);

        assertEquals(ctx2, ongoingTask.getCtx().get(ctx2));
        assertEquals(2, ongoingTask.getMetric().get(metric2).intValue());
        assertEquals(text2, ongoingTask.getText().get(text2));
        assertEquals(string2, ongoingTask.getString().get(string2));
        String[] split = ongoingTask.getLog().split("\n");
        assertEquals(3, split.length);
    }

    @TimberLogTask(name = EVENT)
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

    @TimberLogTask(name = EVENT + '2')
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

        TimberLogTest.waitForTask(parentTaskId, TaskStatus.SUCCESS);
        TimberLogTest.waitForTask(taskId1Arr[0], TaskStatus.SUCCESS);

        Task parentTask = client.getTaskById(parentTaskId);
        assertTaskPrimary(parentTask, ongoingTaskName, TaskStatus.SUCCESS, parentTaskId, true, true);
        assertEquals(ctx1, parentTask.getCtx().get(ctx1));

        Task childTask = client.getTaskById(taskId1Arr[0]);
        assertTask(childTask, EVENT + '2', true, true, parentTaskId, parentTaskId, TaskStatus.SUCCESS, ongoingTaskName);
        assertEquals(ctx1, childTask.getCtx().get(ctx1));
    }

    @TimberLogTask(name = EVENT + '2')
    private String testOngoingPrimaryTask2() {
        return TimberLogger.getCurrentTaskId();

    }

    public void testOngoingTaskWithContext() {
        final String[] taskIdArr = new String[1];


        String ongoingTaskName = EVENT + '1';

        String ongoingTaskId = TimberLoggerAdvanced.startWithDateToDelete(ongoingTaskName, ZonedDateTime.now().minusDays(1));

        new Thread(() -> {
            try (TimberLogContext ignored = new TimberLogContext(ongoingTaskId)) {
                taskIdArr[0] = testOngoingTaskWithContext2();
            } catch (Exception ignored) {

            }
        }).run();

        TimberLoggerAdvanced.success(ongoingTaskId, LogParams.create());

        String childTaskId = taskIdArr[0];

        TimberLogTest.waitForTask(ongoingTaskId, TaskStatus.SUCCESS);
        TimberLogTest.waitForTask(childTaskId, TaskStatus.SUCCESS);

        Task childTask = client.getTaskById(childTaskId);
        assertTask(childTask, EVENT + '2', true, true, ongoingTaskId, ongoingTaskId, TaskStatus.SUCCESS, ongoingTaskName);

        Task ongoingTask = client.getTaskById(ongoingTaskId);
        assertTaskPrimary(ongoingTask,  ongoingTaskName, TaskStatus.SUCCESS, ongoingTaskId, true, true);
    }

    @TimberLogTask(name = EVENT + '2')
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
        TimberLogTest.waitForTask(finalTaskId, TaskStatus.SUCCESS);

        Task childTask = client.getTaskById(taskId);
        assertTaskPrimary(childTask, EVENT + '2', TaskStatus.SUCCESS, taskId, true, true);
    }

    @TimberLogTask(name = EVENT + '2')
    private String testOngoingTaskWithNullContext2() {
        return TimberLogger.getCurrentTaskId();
    }

    void testIncorrectTaskStartSuccessStartSuccess(boolean withUpdate) throws InterruptedException {
        String taskId = TimberLoggerAdvanced.start(EVENT);
        TimberLoggerAdvanced.success(taskId);
        if (withUpdate) {
            Thread.sleep(3000);
        }
        else {
            Thread.sleep(3);
        }
        TimberLoggerAdvanced.start(taskId, EVENT, null, LogParams.create());
        TimberLoggerAdvanced.success(taskId);

        TimberLogTest.waitForTask(taskId, TaskStatus.CORRUPTED);

        Task task = client.getTaskById(taskId);
        assertCorrupted(task, null);
    }

    private void assertCorrupted(Task task, String reason) {
        assertEquals(TaskStatus.CORRUPTED, task.getStatus());
        if (reason != null) {
            assertEquals(reason, task.getString().get(CORRUPTED_REASON));
        }
    }

    void testIncorrectTaskStartSuccessStart(boolean withUpdate) throws InterruptedException {
        String taskId = TimberLoggerAdvanced.start(EVENT);
        TimberLoggerAdvanced.success(taskId);
        if (withUpdate) {
            Thread.sleep(3000);
        }
        else {
            Thread.sleep(3);
        }
        TimberLoggerAdvanced.start(taskId, EVENT, null, LogParams.create());

        TimberLogTest.waitForTask(taskId, TaskStatus.CORRUPTED);

        Task task = client.getTaskById(taskId);
        assertCorrupted(task, StartEvent.ALREADY_STARTED_DIFFERENT_START_TIME);
    }

    void testIncorrectTaskStartSuccessSuccess(boolean withUpdate) throws InterruptedException {
        String taskId = TimberLoggerAdvanced.start(EVENT);
        TimberLoggerAdvanced.success(taskId);
        if (withUpdate) {
            Thread.sleep(3000);
        }
        else {
            Thread.sleep(3);
        }
        TimberLoggerAdvanced.success(taskId);

        TimberLogTest.waitForTask(taskId, TaskStatus.CORRUPTED);

        Task task = client.getTaskById(taskId);
        assertCorrupted(task, ALREADY_CLOSED_DIFFERENT_CLOSE_TIME);
    }

    void testIncorrectTaskStartSuccessError(boolean withUpdate) throws InterruptedException {
        String taskId = TimberLoggerAdvanced.start(EVENT);
        TimberLoggerAdvanced.success(taskId);
        if (withUpdate) {
            Thread.sleep(3000);
        }
        else {
            Thread.sleep(3);
        }
        TimberLoggerAdvanced.success(taskId);

        TimberLogTest.waitForTask(taskId, TaskStatus.CORRUPTED);

        Task task = client.getTaskById(taskId);
        assertCorrupted(task, ALREADY_CLOSED_DIFFERENT_CLOSE_TIME);
    }

    void testIncorrectTaskStartStartSuccess(boolean withUpdate) throws InterruptedException {
        String taskId = TimberLoggerAdvanced.start(EVENT);
        if (withUpdate) {
            Thread.sleep(3000);
        }
        else {
            Thread.sleep(3);
        }
        TimberLoggerAdvanced.start(taskId, EVENT, null, LogParams.create());
        TimberLoggerAdvanced.success(taskId);

        TimberLogTest.waitForTask(taskId, TaskStatus.CORRUPTED);

        Task task = client.getTaskById(taskId);
        assertCorrupted(task, StartEvent.ALREADY_STARTED_DIFFERENT_START_TIME);
    }

    void testIncorrectTaskStartStart(boolean withUpdate) throws InterruptedException {
        String taskId = TimberLoggerAdvanced.start(EVENT);
        if (withUpdate) {
            Thread.sleep(3000);
        }
        else {
            Thread.sleep(3);
        }
        TimberLoggerAdvanced.start(taskId, EVENT, null, LogParams.create());

        TimberLogTest.waitForTask(taskId, TaskStatus.CORRUPTED);

        Task task = client.getTaskById(taskId);
        assertCorrupted(task, StartEvent.ALREADY_STARTED_DIFFERENT_START_TIME);
    }

    void testIncorrectTaskSuccessStartSuccess(boolean withUpdate) throws InterruptedException {
        String id = generateTaskId(EVENT);
        TimberLoggerAdvanced.success(id);
        if (withUpdate) {
            Thread.sleep(3000);
        }
        else {
            Thread.sleep(3);
        }
        String taskId = TimberLoggerAdvanced.start(id, EVENT, null, LogParams.create());
        TimberLoggerAdvanced.success(id);

        TimberLogTest.waitForTask(taskId, TaskStatus.CORRUPTED);

        Task task = client.getTaskById(taskId);
        assertCorrupted(task, ALREADY_CLOSED_DIFFERENT_CLOSE_TIME);
    }

    void testIncorrectTaskSuccessSuccess(boolean withUpdate) throws InterruptedException {
        String taskId = generateTaskId(EVENT);
        TimberLoggerAdvanced.success(taskId);
        if (withUpdate) {
            Thread.sleep(3000);
        }
        else {
            Thread.sleep(3);
        }
        TimberLoggerAdvanced.success(taskId);

        TimberLogTest.waitForTask(taskId, TaskStatus.CORRUPTED);

        Task task = client.getTaskById(taskId);
        assertCorrupted(task, ALREADY_CLOSED_DIFFERENT_CLOSE_TIME);
    }

    void testIncorrectTaskSuccessError(boolean withUpdate) throws InterruptedException {
        String taskId = generateTaskId(EVENT);
        TimberLoggerAdvanced.success(taskId);
        if (withUpdate) {
            Thread.sleep(3000);
        }
        else {
            Thread.sleep(3);
        }
        TimberLoggerAdvanced.error(taskId, new Exception());

        TimberLogTest.waitForTask(taskId, TaskStatus.CORRUPTED);

        Task task = client.getTaskById(taskId);
        assertCorrupted(task, ALREADY_CLOSED_DIFFERENT_CLOSE_STATUS);
    }

    void testIncorrectTaskErrorStartSuccess(boolean withUpdate) throws InterruptedException {
        String taskId = generateTaskId(EVENT);
        TimberLoggerAdvanced.error(taskId, new Exception());
        if (withUpdate) {
            Thread.sleep(3000);
        }
        else {
            Thread.sleep(3);
        }
        TimberLoggerAdvanced.start(taskId, EVENT, null, LogParams.create());
        TimberLoggerAdvanced.success(taskId);

        TimberLogTest.waitForTask(taskId, TaskStatus.CORRUPTED);

        Task task = client.getTaskById(taskId);
        assertCorrupted(task, ALREADY_CLOSED_DIFFERENT_CLOSE_STATUS);
    }

    void testIncorrectTaskErrorSuccess(boolean withUpdate) throws InterruptedException {
        String taskId = generateTaskId(EVENT);
        TimberLoggerAdvanced.error(taskId, new Exception());
        if (withUpdate) {
            Thread.sleep(3000);
        }
        else {
            Thread.sleep(3);
        }
        TimberLoggerAdvanced.success(taskId);

        TimberLogTest.waitForTask(taskId, TaskStatus.CORRUPTED);

        Task task = client.getTaskById(taskId);
        assertCorrupted(task, ALREADY_CLOSED_DIFFERENT_CLOSE_STATUS);
    }

    void testIncorrectTaskErrorError(boolean withUpdate) throws InterruptedException {
        String taskId = generateTaskId(EVENT);
        TimberLoggerAdvanced.error(taskId, new Exception());
        if (withUpdate) {
            Thread.sleep(3000);
        }
        else {
            Thread.sleep(3);
        }
        TimberLoggerAdvanced.error(taskId, new Exception());

        TimberLogTest.waitForTask(taskId, TaskStatus.CORRUPTED);

        Task task = client.getTaskById(taskId);
        assertCorrupted(task, ALREADY_CLOSED_DIFFERENT_CLOSE_TIME);
    }

    void testIncorrectTaskStartErrorStart(boolean withUpdate) throws InterruptedException {
        String taskId = TimberLoggerAdvanced.start(EVENT);
        TimberLoggerAdvanced.error(taskId, new Exception());
        if (withUpdate) {
            Thread.sleep(3000);
        }
        else {
            Thread.sleep(300);
        }
        TimberLoggerAdvanced.start(taskId, EVENT, null, LogParams.create());

        TimberLogTest.waitForTask(taskId, TaskStatus.CORRUPTED);

        Task task = client.getTaskById(taskId);
        assertCorrupted(task, StartEvent.ALREADY_STARTED_DIFFERENT_START_TIME);
    }
}

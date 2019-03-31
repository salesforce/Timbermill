package com.datorama.timbermill;

import com.datorama.timbermill.annotation.TimberLog;
import com.datorama.timbermill.pipe.LocalOutputPipe;
import com.datorama.timbermill.pipe.LocalOutputPipeConfig;
import com.datorama.timbermill.unit.LogParams;
import com.datorama.timbermill.unit.Task;
import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.datorama.timbermill.unit.Task.*;
import static org.junit.Assert.*;
import static org.junit.Assert.assertNull;

public class TimberLogAdvancedTest {

    private static final String EVENT = "Event";
    private static final String TEST = "test";
    private static final String HTTP_LOCALHOST_9200 = "http://localhost:9200";
    private static ElasticsearchClient client = new ElasticsearchClient(TEST, HTTP_LOCALHOST_9200, 1000, 0);


    @BeforeClass
    public static void init() {
        LocalOutputPipeConfig.Builder builder = new LocalOutputPipeConfig.Builder().env(TEST).url(HTTP_LOCALHOST_9200).defaultMaxChars(1000).secondBetweenPolling(1);
        LocalOutputPipeConfig config = new LocalOutputPipeConfig(builder);
        TimberLogger.bootstrap(new LocalOutputPipe(config));
    }

    @AfterClass
    public static void kill() {
        TimberLogger.exit();
    }

    @Test
    public void testOngoingTask() {
        final String[] taskId1Arr = new String[1];
        final String[] taskId2Arr = new String[1];
        final String[] ongoingtaskIdArr = new String[1];

        String ctx1 = "ctx1";
        String ctx2 = "ctx2";
        String metric1 = "metric1";
        String metric2 = "metric2";
        String text1 = "text1";
        String text2 = "text2";

        String ongoingTaskType = EVENT + '1';

        testOngoingTask1(taskId1Arr, taskId2Arr, ongoingtaskIdArr, ctx1, ctx2, metric1, metric2, text1, text2, ongoingTaskType);


        String taskId = taskId1Arr[0];
        String childTaskId = taskId2Arr[0];
        String ongoingTaskId = ongoingtaskIdArr[0];

        Awaitility.await().atMost(90, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId) != null)
                && (client.getTaskById(taskId).getStatus() == TaskStatus.SUCCESS));
        Awaitility.await().atMost(90, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(childTaskId) != null)
                && (client.getTaskById(childTaskId).getStatus() == TaskStatus.SUCCESS));
        Awaitility.await().atMost(90, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(ongoingTaskId) != null)
                && (client.getTaskById(ongoingTaskId).getStatus() == TaskStatus.SUCCESS));

        Task childTask = client.getTaskById(childTaskId);
        assertEquals(taskId, childTask.getParentId());

        Task ongoingTask = client.getTaskById(ongoingTaskId);
        assertEquals(ongoingTaskType, ongoingTask.getName());
        assertEquals(ctx1, ongoingTask.getCtx().get(ctx1));
        assertEquals(ctx2, ongoingTask.getCtx().get(ctx2));
        assertEquals(1, ongoingTask.getMetric().get(metric1).intValue());
        assertEquals(2, ongoingTask.getMetric().get(metric2).intValue());
        assertEquals(text1, ongoingTask.getText().get(text1));
        assertEquals(text2, ongoingTask.getText().get(text2));
        assertEquals(taskId, ongoingTask.getPrimaryId());
        assertEquals(TaskStatus.SUCCESS, ongoingTask.getStatus());
        assertEquals(taskId, ongoingTask.getParentId());
        assertTrue(ongoingTask.getParentsPath().contains(EVENT));
    }

    @TimberLog(name = EVENT)
    private void testOngoingTask1(String[] taskId1Arr, String[] taskId2Arr, String[] ongoingtaskIdArr, String attr1, String attr2, String metric1, String metric2, String data1, String data2, String ongoingTaskType) {
        taskId1Arr[0] = TimberLogger.getCurrentTaskId();

        ongoingtaskIdArr[0] = TimberLogAdvanced.start(ongoingTaskType, taskId1Arr[0], LogParams.create().context(attr1, attr1).text(data1, data1).metric(metric1, 1));

        taskId2Arr[0] = testOngoingTask2();

        new Thread(() -> {
            LogParams logParams = LogParams.create().context(attr2, attr2).metric(metric2, 2).text(data2, data2);
            TimberLogAdvanced.logParams(ongoingtaskIdArr[0], logParams);
            TimberLogAdvanced.success(ongoingtaskIdArr[0]);
        }).run();
    }

    @TimberLog(name = EVENT + '2')
    private String testOngoingTask2() {
        return TimberLogger.getCurrentTaskId();
    }

    @Test
    public void testOngoingPrimaryTask() {
        final String[] taskId1Arr = new String[1];

        String ctx1 = "ctx1";

        String ongoingTaskType = EVENT + '1';

        String parentTaskId = TimberLogAdvanced.start(ongoingTaskType, LogParams.create().context(ctx1, ctx1));

        new Thread(() -> {
            try(TimberLogContext ignored = new TimberLogContext(parentTaskId)) {
                taskId1Arr[0] = testOngoingPrimaryTask2();
            } catch (Exception e) {
                fail(e.getMessage());
            }
        }).run();

        TimberLogAdvanced.success(parentTaskId);

        Awaitility.await().atMost(90, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(parentTaskId) != null)
                && (client.getTaskById(parentTaskId).getStatus() == TaskStatus.SUCCESS));
        Awaitility.await().atMost(90, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(taskId1Arr[0]) != null)
                && (client.getTaskById(taskId1Arr[0]).getStatus() == TaskStatus.SUCCESS));

        Task parentTask = client.getTaskById(parentTaskId);
        Task childTask = client.getTaskById(taskId1Arr[0]);

        assertEquals(ongoingTaskType, parentTask.getName());
        assertEquals(ctx1, parentTask.getCtx().get(ctx1));
        assertEquals(parentTaskId, parentTask.getPrimaryId());
        assertEquals(TaskStatus.SUCCESS, parentTask.getStatus());
        assertNull(parentTask.getParentsPath());

        assertEquals(EVENT + '2', childTask.getName());
        assertEquals(ctx1, childTask.getCtx().get(ctx1));
        assertEquals(parentTaskId, childTask.getPrimaryId());
        assertEquals(TaskStatus.SUCCESS, childTask.getStatus());
        assertEquals(parentTaskId, childTask.getPrimaryId());
        assertEquals(1 ,childTask.getParentsPath().size());
        assertTrue(childTask.getParentsPath().contains(ongoingTaskType));
    }

    @TimberLog(name = EVENT + '2')
    private String testOngoingPrimaryTask2() {
        return TimberLogger.getCurrentTaskId();

    }

    @Test
    public void testOngoingTaskWithContext() {
        final String[] taskIdArr = new String[1];


        String ongoingTaskType = EVENT + '1';

        String ongoingTaskId = TimberLogAdvanced.start(ongoingTaskType);

        new Thread(() -> {
            try (TimberLogContext ignored = new TimberLogContext(ongoingTaskId)) {
                taskIdArr[0] = testOngoingTaskWithContext2();
            } catch (Exception ignored) {

            }
        }).run();

        TimberLogAdvanced.success(ongoingTaskId);

        String childTaskId = taskIdArr[0];

        Awaitility.await().atMost(90, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(ongoingTaskId) != null)
                && (client.getTaskById(ongoingTaskId).getStatus() == TaskStatus.SUCCESS));
        Awaitility.await().atMost(90, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> (client.getTaskById(childTaskId) != null)
                && (client.getTaskById(childTaskId).getStatus() == TaskStatus.SUCCESS));

        Task childTask = client.getTaskById(childTaskId);
        assertEquals(ongoingTaskId, childTask.getParentId());
    }

    @TimberLog(name = EVENT + '2')
    private String testOngoingTaskWithContext2() {
        return TimberLogger.getCurrentTaskId();
    }

    @Test
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
        assertNull(childTask.getParentId());
        assertEquals(taskId, childTask.getPrimaryId());
    }

    @TimberLog(name = EVENT + '2')
    private String testOngoingTaskWithNullContext2() {
        return TimberLogger.getCurrentTaskId();
    }
}

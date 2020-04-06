package com.datorama.oss.timbermill;

import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import com.datorama.oss.timbermill.unit.Event;
import com.datorama.oss.timbermill.unit.LogParams;
import com.datorama.oss.timbermill.unit.Task;
import com.datorama.oss.timbermill.unit.TaskStatus;

import static com.datorama.oss.timbermill.TimberLogTest.waitForTask;
import static com.datorama.oss.timbermill.common.Constants.DEFAULT_ELASTICSEARCH_URL;

public class TimberLogAdvancedOrphansTest {

    private static final String ORPHAN = "orphan";
    private static final String ORPHAN_PARENT = "orphan_parent";
    private static final String CTX = "ctx";
    private static final String ORPHAN_CHILD = "orphan_child";
    private static ElasticsearchClient client;

    @BeforeClass
    public static void setUp() {
        String elasticUrl = System.getenv("ELASTICSEARCH_URL");
        if (StringUtils.isEmpty(elasticUrl)){
            elasticUrl = DEFAULT_ELASTICSEARCH_URL;
        }
        client = new ElasticsearchClient(elasticUrl, 1000, 1, null, null, null,
                7, 100, 1000000000, 3, 3,3,true, null);
    }

    @AfterClass
    public static void kill() {
        TimberLogger.exit();
    }

    public void testOrphanIncorrectOrder() {
        String taskId = Event.generateTaskId(ORPHAN);
        String parentTaskId = Event.generateTaskId(ORPHAN_PARENT);
        TimberLoggerAdvanced.success(taskId);
        TimberLoggerAdvanced.start(taskId, ORPHAN, parentTaskId, LogParams.create());

        waitForTask(taskId, TaskStatus.SUCCESS);
        Task task = client.getTaskById(taskId);
        Assert.assertTrue(task.isOrphan());
    }

    public void testOrphanWithAdoption() {
        String parentTaskId = Event.generateTaskId(ORPHAN_PARENT);
        String taskId = TimberLoggerAdvanced.start(ORPHAN, parentTaskId);
        TimberLoggerAdvanced.success(taskId);
        waitForTask(taskId, TaskStatus.SUCCESS);

        String ctx = CTX;
        TimberLoggerAdvanced.start(parentTaskId, ORPHAN_PARENT, null, LogParams.create().context(ctx, ctx));
        TimberLoggerAdvanced.success(parentTaskId);

        waitForTask(parentTaskId, TaskStatus.SUCCESS);
        Task task = client.getTaskById(taskId);
        Assert.assertFalse(task.isOrphan());
        Assert.assertEquals(parentTaskId, task.getParentId());
        Assert.assertEquals(parentTaskId, task.getPrimaryId());
        Assert.assertEquals(1, task.getParentsPath().size());
        Assert.assertEquals(ORPHAN_PARENT, task.getParentsPath().get(0));
        Assert.assertEquals(ctx, task.getCtx().get(ctx));
    }

    public void testOrphanWithComplexAdoption() {
        String parentTaskId = Event.generateTaskId(ORPHAN_PARENT);
        String taskId = TimberLoggerAdvanced.start(ORPHAN, parentTaskId);
        TimberLoggerAdvanced.success(taskId, LogParams.create().context(CTX + 1, CTX + 1));
        waitForTask(taskId, TaskStatus.SUCCESS);

        String orphanChildId = TimberLoggerAdvanced.start(ORPHAN_CHILD, taskId, LogParams.create().context(CTX + 2, CTX + 2));
        TimberLoggerAdvanced.success(orphanChildId);

        TimberLoggerAdvanced.start(parentTaskId, ORPHAN_PARENT, null, LogParams.create().context(CTX, CTX));
        TimberLoggerAdvanced.success(parentTaskId);



        waitForTask(orphanChildId, TaskStatus.SUCCESS);
        waitForTask(parentTaskId, TaskStatus.SUCCESS);

        Task parentTask = client.getTaskById(parentTaskId);
        Task task = client.getTaskById(taskId);
        Task orphanChildTask = client.getTaskById(orphanChildId);

        Assert.assertNull(parentTask.isOrphan());
        Assert.assertNull(parentTask.getParentId());
        Assert.assertEquals(parentTaskId, parentTask.getPrimaryId());
        Assert.assertEquals(CTX, parentTask.getCtx().get(CTX));

        Assert.assertFalse(task.isOrphan());
        Assert.assertEquals(parentTaskId, task.getParentId());
        Assert.assertEquals(parentTaskId, task.getPrimaryId());
        Assert.assertEquals(1, task.getParentsPath().size());
        Assert.assertEquals(ORPHAN_PARENT, task.getParentsPath().get(0));
        Assert.assertEquals(CTX, task.getCtx().get(CTX));
        Assert.assertEquals(CTX + 1, task.getCtx().get(CTX + 1));

        Assert.assertFalse(orphanChildTask.isOrphan());
        Assert.assertEquals(taskId, orphanChildTask.getParentId());
        Assert.assertEquals(parentTaskId, orphanChildTask.getPrimaryId());
        Assert.assertEquals(2, orphanChildTask.getParentsPath().size());
        Assert.assertEquals(ORPHAN_PARENT, orphanChildTask.getParentsPath().get(0));
        Assert.assertEquals(ORPHAN, orphanChildTask.getParentsPath().get(1));
        Assert.assertEquals(CTX, orphanChildTask.getCtx().get(CTX));
        Assert.assertEquals(CTX + 1, orphanChildTask.getCtx().get(CTX + 1));
        Assert.assertEquals(CTX + 2, orphanChildTask.getCtx().get(CTX + 2));
    }

    public void testOutOfOrderComplexOrphanWithAdoption() {
        String orphan3TaskId = Event.generateTaskId(ORPHAN +"3");

        String ctx = CTX;
        String orphan41TaskId = TimberLoggerAdvanced.start(ORPHAN +"41", orphan3TaskId);
        TimberLoggerAdvanced.logParams(orphan41TaskId, LogParams.create().context(ctx + "41", ctx + "41"));
        TimberLoggerAdvanced.success(orphan41TaskId);

        String orphan42TaskId = TimberLoggerAdvanced.start(ORPHAN +"42", orphan3TaskId);
        TimberLoggerAdvanced.logParams(orphan42TaskId, LogParams.create().context(ctx + "42", ctx + "42"));
        TimberLoggerAdvanced.success(orphan42TaskId);

        waitForTask(orphan41TaskId, TaskStatus.SUCCESS);
        waitForTask(orphan42TaskId, TaskStatus.SUCCESS);
        Task task41 = client.getTaskById(orphan41TaskId);
        Task task42 = client.getTaskById(orphan42TaskId);

        Assert.assertTrue(task41.isOrphan());
        Assert.assertEquals(orphan3TaskId, task41.getParentId());
        Assert.assertNull(task41.getPrimaryId());
        Assert.assertEquals(ctx + "41", task41.getCtx().get(ctx +"41"));

        Assert.assertTrue(task42.isOrphan());
        Assert.assertEquals(orphan3TaskId, task42.getParentId());
        Assert.assertNull(task42.getPrimaryId());
        Assert.assertEquals(ctx + "42", task42.getCtx().get(ctx +"42"));

        String orphan2TaskId = Event.generateTaskId(ORPHAN +"2");
        TimberLoggerAdvanced.start(orphan3TaskId, ORPHAN +"3", orphan2TaskId, LogParams.create().context(ctx + "3", ctx + "3"));
        TimberLoggerAdvanced.success(orphan3TaskId);

        waitForTask(orphan3TaskId, TaskStatus.SUCCESS);
        Task task3 = client.getTaskById(orphan3TaskId);

        Assert.assertTrue(task3.isOrphan());
        Assert.assertEquals(orphan2TaskId, task3.getParentId());
        Assert.assertEquals(ctx + "3", task3.getCtx().get(ctx +"3"));

        String orphan1TaskId = Event.generateTaskId(ORPHAN +"1");
        TimberLoggerAdvanced.start(orphan1TaskId, ORPHAN +"1", null, LogParams.create().context(ctx + "1", ctx + "1"));
        TimberLoggerAdvanced.success(orphan1TaskId);

        waitForTask(orphan1TaskId, TaskStatus.SUCCESS);

        TimberLoggerAdvanced.start(orphan2TaskId, ORPHAN +"2", orphan1TaskId, LogParams.create().context(ctx + "2", ctx + "2"));
        TimberLoggerAdvanced.success(orphan2TaskId);

        waitForTask(orphan2TaskId, TaskStatus.SUCCESS);

        Task task1 = client.getTaskById(orphan1TaskId);
        Task task2 = client.getTaskById(orphan2TaskId);
        task3 = client.getTaskById(orphan3TaskId);
        task41 = client.getTaskById(orphan41TaskId);
        task42 = client.getTaskById(orphan42TaskId);

        Assert.assertNull(task1.isOrphan());
        Assert.assertNull(task1.getParentId());
        Assert.assertEquals(orphan1TaskId, task1.getPrimaryId());
        Assert.assertNull(task1.getParentsPath());
        Assert.assertEquals(ctx + "1", task1.getCtx().get(ctx +"1"));

        Assert.assertTrue(task2.isOrphan() == null || !task2.isOrphan());
        Assert.assertEquals(orphan1TaskId, task2.getParentId());
        Assert.assertEquals(orphan1TaskId, task2.getPrimaryId());
        Assert.assertEquals(1, task2.getParentsPath().size());
        Assert.assertEquals(ORPHAN +"1", task2.getParentsPath().get(0));
        Assert.assertEquals(ctx + "1", task2.getCtx().get(ctx +"1"));
        Assert.assertEquals(ctx + "2", task2.getCtx().get(ctx +"2"));

        Assert.assertFalse(task3.isOrphan());
        Assert.assertEquals(orphan2TaskId, task3.getParentId());
        Assert.assertEquals(orphan1TaskId, task3.getPrimaryId());
        Assert.assertEquals(2, task3.getParentsPath().size());
        Assert.assertEquals(ORPHAN +"1", task3.getParentsPath().get(0));
        Assert.assertEquals(ORPHAN +"2", task3.getParentsPath().get(1));
        Assert.assertEquals(ctx + "1", task3.getCtx().get(ctx +"1"));
        Assert.assertEquals(ctx + "2", task3.getCtx().get(ctx +"2"));
        Assert.assertEquals(ctx + "3", task3.getCtx().get(ctx +"3"));

        Assert.assertFalse(task41.isOrphan());
        Assert.assertEquals(orphan3TaskId, task41.getParentId());
        Assert.assertEquals(orphan1TaskId, task41.getPrimaryId());
        Assert.assertEquals(3, task41.getParentsPath().size());
        Assert.assertEquals(ORPHAN +"1", task41.getParentsPath().get(0));
        Assert.assertEquals(ORPHAN +"2", task41.getParentsPath().get(1));
        Assert.assertEquals(ORPHAN +"3", task41.getParentsPath().get(2));
        Assert.assertEquals(ctx + "1", task41.getCtx().get(ctx +"1"));
        Assert.assertEquals(ctx + "2", task41.getCtx().get(ctx +"2"));
        Assert.assertEquals(ctx + "3", task41.getCtx().get(ctx +"3"));
        Assert.assertEquals(ctx + "41", task41.getCtx().get(ctx +"41"));
        Assert.assertNull(task41.getCtx().get(ctx +"42"));

        Assert.assertFalse(task42.isOrphan());
        Assert.assertEquals(orphan3TaskId, task42.getParentId());
        Assert.assertEquals(orphan1TaskId, task42.getPrimaryId());
        Assert.assertEquals(3, task42.getParentsPath().size());
        Assert.assertEquals(ORPHAN +"1", task42.getParentsPath().get(0));
        Assert.assertEquals(ORPHAN +"2", task42.getParentsPath().get(1));
        Assert.assertEquals(ORPHAN +"3", task42.getParentsPath().get(2));
        Assert.assertEquals(ctx + "1", task42.getCtx().get(ctx +"1"));
        Assert.assertEquals(ctx + "2", task42.getCtx().get(ctx +"2"));
        Assert.assertEquals(ctx + "3", task42.getCtx().get(ctx +"3"));
        Assert.assertEquals(ctx + "42", task42.getCtx().get(ctx +"42"));
        Assert.assertNull(task42.getCtx().get(ctx +"41"));
    }

    public void testInOrderComplexOrphanWithAdoption() {
        String orphan1TaskId = Event.generateTaskId(ORPHAN +"1");
        TimberLoggerAdvanced.start(orphan1TaskId, ORPHAN +"1", null, LogParams.create().context(CTX + "1", CTX + "1"));
        TimberLoggerAdvanced.success(orphan1TaskId);

        String orphan2TaskId = Event.generateTaskId(ORPHAN +"2");

        String orphan3TaskId = Event.generateTaskId(ORPHAN +"3");
        TimberLoggerAdvanced.start(orphan3TaskId, ORPHAN +"3", orphan2TaskId, LogParams.create().context(CTX + "3", CTX + "3"));
        TimberLoggerAdvanced.success(orphan3TaskId);

        String orphan4TaskId = Event.generateTaskId(ORPHAN +"4");

        String orphan5TaskId = Event.generateTaskId(ORPHAN +"5");
        TimberLoggerAdvanced.start(orphan5TaskId, ORPHAN +"5", orphan4TaskId, LogParams.create().context(CTX + "5", CTX + "5"));
        TimberLoggerAdvanced.success(orphan5TaskId);

        String orphan6TaskId = Event.generateTaskId(ORPHAN +"6");

        String orphan7TaskId = Event.generateTaskId(ORPHAN +"7");
        TimberLoggerAdvanced.start(orphan7TaskId, ORPHAN +"7", orphan6TaskId, LogParams.create().context(CTX + "7", CTX + "7"));
        TimberLoggerAdvanced.success(orphan7TaskId);

        waitForTask(orphan1TaskId, TaskStatus.SUCCESS);
        waitForTask(orphan3TaskId, TaskStatus.SUCCESS);
        waitForTask(orphan5TaskId, TaskStatus.SUCCESS);
        waitForTask(orphan7TaskId, TaskStatus.SUCCESS);


        Task task1 = client.getTaskById(orphan1TaskId);
        Task task3 = client.getTaskById(orphan3TaskId);
        Task task5 = client.getTaskById(orphan5TaskId);
        Task task7 = client.getTaskById(orphan7TaskId);

        Assert.assertNull(task1.isOrphan());
        Assert.assertNull(task1.getParentId());
        Assert.assertEquals(orphan1TaskId, task1.getPrimaryId());
        Assert.assertNull(task1.getParentsPath());
        Assert.assertEquals(CTX + "1", task1.getCtx().get(CTX +"1"));

        Assert.assertTrue(task3.isOrphan());
        Assert.assertEquals(orphan2TaskId, task3.getParentId());
        Assert.assertNull(task3.getPrimaryId());
        Assert.assertEquals(CTX + "3", task3.getCtx().get(CTX +"3"));

        Assert.assertTrue(task5.isOrphan());
        Assert.assertEquals(orphan4TaskId, task5.getParentId());
        Assert.assertNull(task5.getPrimaryId());
        Assert.assertEquals(CTX + "5", task5.getCtx().get(CTX +"5"));

        Assert.assertTrue(task7.isOrphan());
        Assert.assertEquals(orphan6TaskId, task7.getParentId());
        Assert.assertNull(task7.getPrimaryId());
        Assert.assertEquals(CTX + "7", task7.getCtx().get(CTX +"7"));


        TimberLoggerAdvanced.start(orphan2TaskId, ORPHAN +"2", orphan1TaskId, LogParams.create().context(CTX + "2", CTX + "2"));
        TimberLoggerAdvanced.success(orphan2TaskId);

        TimberLoggerAdvanced.start(orphan4TaskId, ORPHAN +"4", orphan3TaskId, LogParams.create().context(CTX + "4", CTX + "4"));
        TimberLoggerAdvanced.success(orphan4TaskId);

        TimberLoggerAdvanced.start(orphan6TaskId, ORPHAN +"6", orphan5TaskId, LogParams.create().context(CTX + "6", CTX + "6"));
        TimberLoggerAdvanced.success(orphan6TaskId);

        waitForTask(orphan2TaskId, TaskStatus.SUCCESS);
        waitForTask(orphan4TaskId, TaskStatus.SUCCESS);
        waitForTask(orphan6TaskId, TaskStatus.SUCCESS);

        task1 = client.getTaskById(orphan1TaskId);
        Task task2 = client.getTaskById(orphan2TaskId);
        task3 = client.getTaskById(orphan3TaskId);
        Task task4 = client.getTaskById(orphan4TaskId);
        task5 = client.getTaskById(orphan5TaskId);
        Task task6 = client.getTaskById(orphan6TaskId);
        task7 = client.getTaskById(orphan7TaskId);

        Assert.assertNull(task1.isOrphan());
        Assert.assertNull(task1.getParentId());
        Assert.assertEquals(orphan1TaskId, task1.getPrimaryId());
        Assert.assertNull(task1.getParentsPath());
        Assert.assertEquals(CTX + "1", task1.getCtx().get(CTX +"1"));

        Assert.assertTrue(task2.isOrphan() == null || !task2.isOrphan());
        Assert.assertEquals(orphan1TaskId, task2.getParentId());
        Assert.assertEquals(orphan1TaskId, task2.getPrimaryId());
        Assert.assertEquals(1, task2.getParentsPath().size());
        Assert.assertEquals(ORPHAN +"1", task2.getParentsPath().get(0));
        Assert.assertEquals(CTX + "1", task2.getCtx().get(CTX +"1"));
        Assert.assertEquals(CTX + "2", task2.getCtx().get(CTX +"2"));

        Assert.assertFalse(task3.isOrphan());
        Assert.assertEquals(orphan2TaskId, task3.getParentId());
        Assert.assertEquals(orphan1TaskId, task3.getPrimaryId());
        Assert.assertEquals(2, task3.getParentsPath().size());
        Assert.assertEquals(ORPHAN +"1", task3.getParentsPath().get(0));
        Assert.assertEquals(ORPHAN +"2", task3.getParentsPath().get(1));
        Assert.assertEquals(CTX + "1", task3.getCtx().get(CTX +"1"));
        Assert.assertEquals(CTX + "2", task3.getCtx().get(CTX +"2"));
        Assert.assertEquals(CTX + "3", task3.getCtx().get(CTX +"3"));

        Assert.assertFalse(task4.isOrphan());
        Assert.assertEquals(orphan3TaskId, task4.getParentId());
        Assert.assertEquals(orphan1TaskId, task4.getPrimaryId());
        Assert.assertEquals(3, task4.getParentsPath().size());
        Assert.assertEquals(ORPHAN +"1", task4.getParentsPath().get(0));
        Assert.assertEquals(ORPHAN +"2", task4.getParentsPath().get(1));
        Assert.assertEquals(ORPHAN +"3", task4.getParentsPath().get(2));
        Assert.assertEquals(CTX + "1", task4.getCtx().get(CTX +"1"));
        Assert.assertEquals(CTX + "2", task4.getCtx().get(CTX +"2"));
        Assert.assertEquals(CTX + "3", task4.getCtx().get(CTX +"3"));
        Assert.assertEquals(CTX + "4", task4.getCtx().get(CTX +"4"));

        Assert.assertFalse(task5.isOrphan());
        Assert.assertEquals(orphan4TaskId, task5.getParentId());
        Assert.assertEquals(orphan1TaskId, task5.getPrimaryId());
        Assert.assertEquals(4, task5.getParentsPath().size());
        Assert.assertEquals(ORPHAN +"1", task5.getParentsPath().get(0));
        Assert.assertEquals(ORPHAN +"2", task5.getParentsPath().get(1));
        Assert.assertEquals(ORPHAN +"3", task5.getParentsPath().get(2));
        Assert.assertEquals(ORPHAN +"4", task5.getParentsPath().get(3));
        Assert.assertEquals(CTX + "1", task5.getCtx().get(CTX +"1"));
        Assert.assertEquals(CTX + "2", task5.getCtx().get(CTX +"2"));
        Assert.assertEquals(CTX + "3", task5.getCtx().get(CTX +"3"));
        Assert.assertEquals(CTX + "4", task5.getCtx().get(CTX +"4"));
        Assert.assertEquals(CTX + "5", task5.getCtx().get(CTX +"5"));

        Assert.assertFalse(task6.isOrphan());
        Assert.assertEquals(orphan5TaskId, task6.getParentId());
        Assert.assertEquals(orphan1TaskId, task6.getPrimaryId());
        Assert.assertEquals(5, task6.getParentsPath().size());
        Assert.assertEquals(ORPHAN +"1", task6.getParentsPath().get(0));
        Assert.assertEquals(ORPHAN +"2", task6.getParentsPath().get(1));
        Assert.assertEquals(ORPHAN +"3", task6.getParentsPath().get(2));
        Assert.assertEquals(ORPHAN +"4", task6.getParentsPath().get(3));
        Assert.assertEquals(ORPHAN +"5", task6.getParentsPath().get(4));
        Assert.assertEquals(CTX + "1", task6.getCtx().get(CTX +"1"));
        Assert.assertEquals(CTX + "2", task6.getCtx().get(CTX +"2"));
        Assert.assertEquals(CTX + "3", task6.getCtx().get(CTX +"3"));
        Assert.assertEquals(CTX + "4", task6.getCtx().get(CTX +"4"));
        Assert.assertEquals(CTX + "5", task6.getCtx().get(CTX +"5"));
        Assert.assertEquals(CTX + "6", task6.getCtx().get(CTX +"6"));

        Assert.assertFalse(task7.isOrphan());
        Assert.assertEquals(orphan6TaskId, task7.getParentId());
        Assert.assertEquals(orphan1TaskId, task7.getPrimaryId());
        Assert.assertEquals(6, task7.getParentsPath().size());
        Assert.assertEquals(ORPHAN +"1", task7.getParentsPath().get(0));
        Assert.assertEquals(ORPHAN +"2", task7.getParentsPath().get(1));
        Assert.assertEquals(ORPHAN +"3", task7.getParentsPath().get(2));
        Assert.assertEquals(ORPHAN +"4", task7.getParentsPath().get(3));
        Assert.assertEquals(ORPHAN +"5", task7.getParentsPath().get(4));
        Assert.assertEquals(ORPHAN +"6", task7.getParentsPath().get(5));
        Assert.assertEquals(CTX + "1", task7.getCtx().get(CTX +"1"));
        Assert.assertEquals(CTX + "2", task7.getCtx().get(CTX +"2"));
        Assert.assertEquals(CTX + "3", task7.getCtx().get(CTX +"3"));
        Assert.assertEquals(CTX + "4", task7.getCtx().get(CTX +"4"));
        Assert.assertEquals(CTX + "5", task7.getCtx().get(CTX +"5"));
        Assert.assertEquals(CTX + "6", task7.getCtx().get(CTX +"6"));
        Assert.assertEquals(CTX + "7", task7.getCtx().get(CTX +"7"));
    }

    public void testOrphanWithAdoptionDifferentBatches() {
        String orphan1TaskId = Event.generateTaskId(ORPHAN +"1");
        TimberLoggerAdvanced.start(orphan1TaskId, ORPHAN +"1", null, LogParams.create().context(CTX + "1", CTX + "1"));
        waitForTask(orphan1TaskId, TaskStatus.UNTERMINATED);
        TimberLoggerAdvanced.success(orphan1TaskId);

        String orphan2TaskId = Event.generateTaskId(ORPHAN +"2");

        String orphan3TaskId = Event.generateTaskId(ORPHAN +"3");
        TimberLoggerAdvanced.start(orphan3TaskId, ORPHAN +"3", orphan2TaskId, LogParams.create().context(CTX + "3", CTX + "3"));
        TimberLoggerAdvanced.success(orphan3TaskId);

        String orphan4TaskId = Event.generateTaskId(ORPHAN +"4");

        String orphan5TaskId = Event.generateTaskId(ORPHAN +"5");
        TimberLoggerAdvanced.start(orphan5TaskId, ORPHAN +"5", orphan4TaskId, LogParams.create().context(CTX + "5", CTX + "5"));
        TimberLoggerAdvanced.success(orphan5TaskId);

        String orphan6TaskId = Event.generateTaskId(ORPHAN +"6");

        String orphan7TaskId = Event.generateTaskId(ORPHAN +"7");
        TimberLoggerAdvanced.start(orphan7TaskId, ORPHAN +"7", orphan6TaskId, LogParams.create().context(CTX + "7", CTX + "7"));
        TimberLoggerAdvanced.success(orphan7TaskId);

        waitForTask(orphan1TaskId, TaskStatus.SUCCESS);
        waitForTask(orphan3TaskId, TaskStatus.SUCCESS);
        waitForTask(orphan5TaskId, TaskStatus.SUCCESS);
        waitForTask(orphan7TaskId, TaskStatus.SUCCESS);


        Task task1 = client.getTaskById(orphan1TaskId);
        Task task3 = client.getTaskById(orphan3TaskId);
        Task task5 = client.getTaskById(orphan5TaskId);
        Task task7 = client.getTaskById(orphan7TaskId);

        Assert.assertNull(task1.isOrphan());
        Assert.assertNull(task1.getParentId());
        Assert.assertEquals(orphan1TaskId, task1.getPrimaryId());
        Assert.assertNull(task1.getParentsPath());
        Assert.assertEquals(CTX + "1", task1.getCtx().get(CTX +"1"));

        Assert.assertTrue(task3.isOrphan());
        Assert.assertEquals(orphan2TaskId, task3.getParentId());
        Assert.assertNull(task3.getPrimaryId());
        Assert.assertEquals(CTX + "3", task3.getCtx().get(CTX +"3"));

        Assert.assertTrue(task5.isOrphan());
        Assert.assertEquals(orphan4TaskId, task5.getParentId());
        Assert.assertNull(task5.getPrimaryId());
        Assert.assertEquals(CTX + "5", task5.getCtx().get(CTX +"5"));

        Assert.assertTrue(task7.isOrphan());
        Assert.assertEquals(orphan6TaskId, task7.getParentId());
        Assert.assertNull(task7.getPrimaryId());
        Assert.assertEquals(CTX + "7", task7.getCtx().get(CTX +"7"));


        TimberLoggerAdvanced.start(orphan2TaskId, ORPHAN +"2", orphan1TaskId, LogParams.create().context(CTX + "2", CTX + "2"));
        TimberLoggerAdvanced.success(orphan2TaskId);

        waitForTask(orphan2TaskId, TaskStatus.SUCCESS);

        TimberLoggerAdvanced.start(orphan4TaskId, ORPHAN +"4", orphan3TaskId, LogParams.create().context(CTX + "4", CTX + "4"));
        TimberLoggerAdvanced.success(orphan4TaskId);

        TimberLoggerAdvanced.start(orphan6TaskId, ORPHAN +"6", orphan5TaskId, LogParams.create().context(CTX + "6", CTX + "6"));
        TimberLoggerAdvanced.success(orphan6TaskId);

        waitForTask(orphan4TaskId, TaskStatus.SUCCESS);
        waitForTask(orphan6TaskId, TaskStatus.SUCCESS);

        task1 = client.getTaskById(orphan1TaskId);
        Task task2 = client.getTaskById(orphan2TaskId);
        task3 = client.getTaskById(orphan3TaskId);
        Task task4 = client.getTaskById(orphan4TaskId);
        task5 = client.getTaskById(orphan5TaskId);
        Task task6 = client.getTaskById(orphan6TaskId);
        task7 = client.getTaskById(orphan7TaskId);

        Assert.assertNull(task1.isOrphan());
        Assert.assertNull(task1.getParentId());
        Assert.assertEquals(orphan1TaskId, task1.getPrimaryId());
        Assert.assertNull(task1.getParentsPath());
        Assert.assertEquals(CTX + "1", task1.getCtx().get(CTX +"1"));

        Assert.assertTrue(task2.isOrphan() == null || !task2.isOrphan());
        Assert.assertEquals(orphan1TaskId, task2.getParentId());
        Assert.assertEquals(orphan1TaskId, task2.getPrimaryId());
        Assert.assertEquals(1, task2.getParentsPath().size());
        Assert.assertEquals(ORPHAN +"1", task2.getParentsPath().get(0));
        Assert.assertEquals(CTX + "1", task2.getCtx().get(CTX +"1"));
        Assert.assertEquals(CTX + "2", task2.getCtx().get(CTX +"2"));

        Assert.assertFalse(task3.isOrphan());
        Assert.assertEquals(orphan2TaskId, task3.getParentId());
        Assert.assertEquals(orphan1TaskId, task3.getPrimaryId());
        Assert.assertEquals(2, task3.getParentsPath().size());
        Assert.assertEquals(ORPHAN +"1", task3.getParentsPath().get(0));
        Assert.assertEquals(ORPHAN +"2", task3.getParentsPath().get(1));
        Assert.assertEquals(CTX + "1", task3.getCtx().get(CTX +"1"));
        Assert.assertEquals(CTX + "2", task3.getCtx().get(CTX +"2"));
        Assert.assertEquals(CTX + "3", task3.getCtx().get(CTX +"3"));

        Assert.assertFalse(task4.isOrphan());
        Assert.assertEquals(orphan3TaskId, task4.getParentId());
        Assert.assertEquals(orphan1TaskId, task4.getPrimaryId());
        Assert.assertEquals(3, task4.getParentsPath().size());
        Assert.assertEquals(ORPHAN +"1", task4.getParentsPath().get(0));
        Assert.assertEquals(ORPHAN +"2", task4.getParentsPath().get(1));
        Assert.assertEquals(ORPHAN +"3", task4.getParentsPath().get(2));
        Assert.assertEquals(CTX + "1", task4.getCtx().get(CTX +"1"));
        Assert.assertEquals(CTX + "2", task4.getCtx().get(CTX +"2"));
        Assert.assertEquals(CTX + "3", task4.getCtx().get(CTX +"3"));
        Assert.assertEquals(CTX + "4", task4.getCtx().get(CTX +"4"));

        Assert.assertFalse(task5.isOrphan());
        Assert.assertEquals(orphan4TaskId, task5.getParentId());
        Assert.assertEquals(orphan1TaskId, task5.getPrimaryId());
        Assert.assertEquals(4, task5.getParentsPath().size());
        Assert.assertEquals(ORPHAN +"1", task5.getParentsPath().get(0));
        Assert.assertEquals(ORPHAN +"2", task5.getParentsPath().get(1));
        Assert.assertEquals(ORPHAN +"3", task5.getParentsPath().get(2));
        Assert.assertEquals(ORPHAN +"4", task5.getParentsPath().get(3));
        Assert.assertEquals(CTX + "1", task5.getCtx().get(CTX +"1"));
        Assert.assertEquals(CTX + "2", task5.getCtx().get(CTX +"2"));
        Assert.assertEquals(CTX + "3", task5.getCtx().get(CTX +"3"));
        Assert.assertEquals(CTX + "4", task5.getCtx().get(CTX +"4"));
        Assert.assertEquals(CTX + "5", task5.getCtx().get(CTX +"5"));

        Assert.assertFalse(task6.isOrphan());
        Assert.assertEquals(orphan5TaskId, task6.getParentId());
        Assert.assertEquals(orphan1TaskId, task6.getPrimaryId());
        Assert.assertEquals(5, task6.getParentsPath().size());
        Assert.assertEquals(ORPHAN +"1", task6.getParentsPath().get(0));
        Assert.assertEquals(ORPHAN +"2", task6.getParentsPath().get(1));
        Assert.assertEquals(ORPHAN +"3", task6.getParentsPath().get(2));
        Assert.assertEquals(ORPHAN +"4", task6.getParentsPath().get(3));
        Assert.assertEquals(ORPHAN +"5", task6.getParentsPath().get(4));
        Assert.assertEquals(CTX + "1", task6.getCtx().get(CTX +"1"));
        Assert.assertEquals(CTX + "2", task6.getCtx().get(CTX +"2"));
        Assert.assertEquals(CTX + "3", task6.getCtx().get(CTX +"3"));
        Assert.assertEquals(CTX + "4", task6.getCtx().get(CTX +"4"));
        Assert.assertEquals(CTX + "5", task6.getCtx().get(CTX +"5"));
        Assert.assertEquals(CTX + "6", task6.getCtx().get(CTX +"6"));

        Assert.assertFalse(task7.isOrphan());
        Assert.assertEquals(orphan6TaskId, task7.getParentId());
        Assert.assertEquals(orphan1TaskId, task7.getPrimaryId());
        Assert.assertEquals(6, task7.getParentsPath().size());
        Assert.assertEquals(ORPHAN +"1", task7.getParentsPath().get(0));
        Assert.assertEquals(ORPHAN +"2", task7.getParentsPath().get(1));
        Assert.assertEquals(ORPHAN +"3", task7.getParentsPath().get(2));
        Assert.assertEquals(ORPHAN +"4", task7.getParentsPath().get(3));
        Assert.assertEquals(ORPHAN +"5", task7.getParentsPath().get(4));
        Assert.assertEquals(ORPHAN +"6", task7.getParentsPath().get(5));
        Assert.assertEquals(CTX + "1", task7.getCtx().get(CTX +"1"));
        Assert.assertEquals(CTX + "2", task7.getCtx().get(CTX +"2"));
        Assert.assertEquals(CTX + "3", task7.getCtx().get(CTX +"3"));
        Assert.assertEquals(CTX + "4", task7.getCtx().get(CTX +"4"));
        Assert.assertEquals(CTX + "5", task7.getCtx().get(CTX +"5"));
        Assert.assertEquals(CTX + "6", task7.getCtx().get(CTX +"6"));
        Assert.assertEquals(CTX + "7", task7.getCtx().get(CTX +"7"));
    }

    public void testStringOfOrphans() {
        String parentTaskId = null;
        String taskId = null;
        int numberOfIterations = 3;
        for (int i = 0; i < numberOfIterations; i++) {
            if (parentTaskId == null) {
                parentTaskId = Event.generateTaskId(ORPHAN);
                taskId = TimberLoggerAdvanced.start(ORPHAN, parentTaskId);
                waitForTask(taskId, TaskStatus.UNTERMINATED);
                TimberLoggerAdvanced.success(taskId);
            }
            else{
                LogParams context = LogParams.create().context("ctx" + i, "ctx" + i);
                if(i + 1 == numberOfIterations){
                    TimberLoggerAdvanced.start(parentTaskId, ORPHAN,  null, context);
                    TimberLoggerAdvanced.success(parentTaskId);
                }
                else{
                    String newParentTaskId = Event.generateTaskId(ORPHAN);
                    TimberLoggerAdvanced.start(parentTaskId, ORPHAN,  newParentTaskId, context);
                    TimberLoggerAdvanced.success(parentTaskId);
                    parentTaskId = newParentTaskId;
                }
            }
        }
        waitForTask(parentTaskId, TaskStatus.SUCCESS);
        waitForTask(taskId, TaskStatus.SUCCESS);
        Task primaryTask = client.getTaskById(parentTaskId);
        Task childTask = client.getTaskById(taskId);

        Assert.assertEquals(parentTaskId, childTask.getPrimaryId());
        Assert.assertNotNull(childTask.getParentsPath());
        Assert.assertFalse(childTask.getParentsPath().isEmpty());
        Assert.assertFalse(childTask.isOrphan());

        Assert.assertEquals(parentTaskId, primaryTask.getPrimaryId());
        Assert.assertNull(primaryTask.getParentId());
        Assert.assertNull(primaryTask.getParentsPath());
        Assert.assertTrue(primaryTask.isOrphan() == null || !primaryTask.isOrphan());
    }
}

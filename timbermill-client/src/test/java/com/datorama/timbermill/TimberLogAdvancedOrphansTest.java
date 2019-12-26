package com.datorama.timbermill;

import org.junit.AfterClass;

import com.datorama.timbermill.unit.Event;
import com.datorama.timbermill.unit.LogParams;
import com.datorama.timbermill.unit.Task;

import static com.datorama.timbermill.TimberLogTest.client;
import static com.datorama.timbermill.unit.Task.TaskStatus;
import static org.junit.Assert.*;

public class TimberLogAdvancedOrphansTest {

    private static final String ORPHAN = "orphan";
    private static final String ORPHAN_PARENT = "orphan_parent";
    private static final String CTX = "ctx";

    @AfterClass
    public static void kill() {
        TimberLogger.exit();
    }

    public void testOrphanIncorrectOrder() {
        String taskId = Event.generateTaskId(ORPHAN);
        String parentTaskId = Event.generateTaskId(ORPHAN_PARENT);
        TimberLoggerAdvanced.success(taskId);
        TimberLoggerAdvanced.start(taskId, ORPHAN, parentTaskId, LogParams.create());

        TimberLogTest.waitForEvents(taskId, TaskStatus.SUCCESS);
        Task task = client.getTaskById(taskId);
        assertTrue(task.isOrphan());
    }

    public void testOrphanWithAdoption() {
        String parentTaskId = Event.generateTaskId(ORPHAN_PARENT);
        String taskId = TimberLoggerAdvanced.start(ORPHAN, parentTaskId);
        TimberLoggerAdvanced.success(taskId);
        TimberLogTest.waitForEvents(taskId, TaskStatus.SUCCESS);

        String ctx = CTX;
        TimberLoggerAdvanced.start(parentTaskId, ORPHAN_PARENT, null, LogParams.create().context(ctx, ctx));
        TimberLoggerAdvanced.success(parentTaskId);

        TimberLogTest.waitForEvents(parentTaskId, TaskStatus.SUCCESS);
        Task task = client.getTaskById(taskId);
        assertFalse(task.isOrphan());
        assertEquals(parentTaskId, task.getParentId());
        assertEquals(parentTaskId, task.getPrimaryId());
        assertEquals(1, task.getParentsPath().size());
        assertEquals(ORPHAN_PARENT, task.getParentsPath().get(0));
        assertEquals(ctx, task.getCtx().get(ctx));
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

        TimberLogTest.waitForEvents(orphan41TaskId, TaskStatus.SUCCESS);
        TimberLogTest.waitForEvents(orphan42TaskId, TaskStatus.SUCCESS);
        Task task41 = client.getTaskById(orphan41TaskId);
        Task task42 = client.getTaskById(orphan42TaskId);

        assertTrue(task41.isOrphan());
        assertEquals(orphan3TaskId, task41.getParentId());
        assertNull(task41.getPrimaryId());
        assertEquals(ctx + "41", task41.getCtx().get(ctx +"41"));

        assertTrue(task42.isOrphan());
        assertEquals(orphan3TaskId, task42.getParentId());
        assertNull(task42.getPrimaryId());
        assertEquals(ctx + "42", task42.getCtx().get(ctx +"42"));

        String orphan2TaskId = Event.generateTaskId(ORPHAN +"2");
        TimberLoggerAdvanced.start(orphan3TaskId, ORPHAN +"3", orphan2TaskId, LogParams.create().context(ctx + "3", ctx + "3"));
        TimberLoggerAdvanced.success(orphan3TaskId);

        TimberLogTest.waitForEvents(orphan3TaskId, TaskStatus.SUCCESS);
        Task task3 = client.getTaskById(orphan3TaskId);

        assertTrue(task3.isOrphan());
        assertEquals(orphan2TaskId, task3.getParentId());
        assertEquals(ctx + "3", task3.getCtx().get(ctx +"3"));

        String orphan1TaskId = Event.generateTaskId(ORPHAN +"1");
        TimberLoggerAdvanced.start(orphan1TaskId, ORPHAN +"1", null, LogParams.create().context(ctx + "1", ctx + "1"));
        TimberLoggerAdvanced.success(orphan1TaskId);

        TimberLogTest.waitForEvents(orphan1TaskId, TaskStatus.SUCCESS);

        TimberLoggerAdvanced.start(orphan2TaskId, ORPHAN +"2", orphan1TaskId, LogParams.create().context(ctx + "2", ctx + "2"));
        TimberLoggerAdvanced.success(orphan2TaskId);

        TimberLogTest.waitForEvents(orphan2TaskId, TaskStatus.SUCCESS);

        Task task1 = client.getTaskById(orphan1TaskId);
        Task task2 = client.getTaskById(orphan2TaskId);
        task3 = client.getTaskById(orphan3TaskId);
        task41 = client.getTaskById(orphan41TaskId);
        task42 = client.getTaskById(orphan42TaskId);

        assertNull(task1.isOrphan());
        assertNull(task1.getParentId());
        assertEquals(orphan1TaskId, task1.getPrimaryId());
        assertNull(task1.getParentsPath());
        assertEquals(ctx + "1", task1.getCtx().get(ctx +"1"));

        assertTrue(task2.isOrphan() == null || !task2.isOrphan());
        assertEquals(orphan1TaskId, task2.getParentId());
        assertEquals(orphan1TaskId, task2.getPrimaryId());
        assertEquals(1, task2.getParentsPath().size());
        assertEquals(ORPHAN +"1", task2.getParentsPath().get(0));
        assertEquals(ctx + "1", task2.getCtx().get(ctx +"1"));
        assertEquals(ctx + "2", task2.getCtx().get(ctx +"2"));

        assertFalse(task3.isOrphan());
        assertEquals(orphan2TaskId, task3.getParentId());
        assertEquals(orphan1TaskId, task3.getPrimaryId());
        assertEquals(2, task3.getParentsPath().size());
        assertEquals(ORPHAN +"1", task3.getParentsPath().get(0));
        assertEquals(ORPHAN +"2", task3.getParentsPath().get(1));
        assertEquals(ctx + "1", task3.getCtx().get(ctx +"1"));
        assertEquals(ctx + "2", task3.getCtx().get(ctx +"2"));
        assertEquals(ctx + "3", task3.getCtx().get(ctx +"3"));

        assertFalse(task41.isOrphan());
        assertEquals(orphan3TaskId, task41.getParentId());
        assertEquals(orphan1TaskId, task41.getPrimaryId());
        assertEquals(3, task41.getParentsPath().size());
        assertEquals(ORPHAN +"1", task41.getParentsPath().get(0));
        assertEquals(ORPHAN +"2", task41.getParentsPath().get(1));
        assertEquals(ORPHAN +"3", task41.getParentsPath().get(2));
        assertEquals(ctx + "1", task41.getCtx().get(ctx +"1"));
        assertEquals(ctx + "2", task41.getCtx().get(ctx +"2"));
        assertEquals(ctx + "3", task41.getCtx().get(ctx +"3"));
        assertEquals(ctx + "41", task41.getCtx().get(ctx +"41"));
        assertNull(task41.getCtx().get(ctx +"42"));

        assertFalse(task42.isOrphan());
        assertEquals(orphan3TaskId, task42.getParentId());
        assertEquals(orphan1TaskId, task42.getPrimaryId());
        assertEquals(3, task42.getParentsPath().size());
        assertEquals(ORPHAN +"1", task42.getParentsPath().get(0));
        assertEquals(ORPHAN +"2", task42.getParentsPath().get(1));
        assertEquals(ORPHAN +"3", task42.getParentsPath().get(2));
        assertEquals(ctx + "1", task42.getCtx().get(ctx +"1"));
        assertEquals(ctx + "2", task42.getCtx().get(ctx +"2"));
        assertEquals(ctx + "3", task42.getCtx().get(ctx +"3"));
        assertEquals(ctx + "42", task42.getCtx().get(ctx +"42"));
        assertNull(task42.getCtx().get(ctx +"41"));
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

        TimberLogTest.waitForEvents(orphan1TaskId, TaskStatus.SUCCESS);
        TimberLogTest.waitForEvents(orphan3TaskId, TaskStatus.SUCCESS);
        TimberLogTest.waitForEvents(orphan5TaskId, TaskStatus.SUCCESS);
        TimberLogTest.waitForEvents(orphan7TaskId, TaskStatus.SUCCESS);


        Task task1 = client.getTaskById(orphan1TaskId);
        Task task3 = client.getTaskById(orphan3TaskId);
        Task task5 = client.getTaskById(orphan5TaskId);
        Task task7 = client.getTaskById(orphan7TaskId);

        assertNull(task1.isOrphan());
        assertNull(task1.getParentId());
        assertEquals(orphan1TaskId, task1.getPrimaryId());
        assertNull(task1.getParentsPath());
        assertEquals(CTX + "1", task1.getCtx().get(CTX +"1"));

        assertTrue(task3.isOrphan());
        assertEquals(orphan2TaskId, task3.getParentId());
        assertNull(task3.getPrimaryId());
        assertEquals(CTX + "3", task3.getCtx().get(CTX +"3"));

        assertTrue(task5.isOrphan());
        assertEquals(orphan4TaskId, task5.getParentId());
        assertNull(task5.getPrimaryId());
        assertEquals(CTX + "5", task5.getCtx().get(CTX +"5"));

        assertTrue(task7.isOrphan());
        assertEquals(orphan6TaskId, task7.getParentId());
        assertNull(task7.getPrimaryId());
        assertEquals(CTX + "7", task7.getCtx().get(CTX +"7"));


        TimberLoggerAdvanced.start(orphan2TaskId, ORPHAN +"2", orphan1TaskId, LogParams.create().context(CTX + "2", CTX + "2"));
        TimberLoggerAdvanced.success(orphan2TaskId);

        TimberLoggerAdvanced.start(orphan4TaskId, ORPHAN +"4", orphan3TaskId, LogParams.create().context(CTX + "4", CTX + "4"));
        TimberLoggerAdvanced.success(orphan4TaskId);

        TimberLoggerAdvanced.start(orphan6TaskId, ORPHAN +"6", orphan5TaskId, LogParams.create().context(CTX + "6", CTX + "6"));
        TimberLoggerAdvanced.success(orphan6TaskId);

        TimberLogTest.waitForEvents(orphan2TaskId, TaskStatus.SUCCESS);
        TimberLogTest.waitForEvents(orphan4TaskId, TaskStatus.SUCCESS);
        TimberLogTest.waitForEvents(orphan6TaskId, TaskStatus.SUCCESS);

        task1 = client.getTaskById(orphan1TaskId);
        Task task2 = client.getTaskById(orphan2TaskId);
        task3 = client.getTaskById(orphan3TaskId);
        Task task4 = client.getTaskById(orphan4TaskId);
        task5 = client.getTaskById(orphan5TaskId);
        Task task6 = client.getTaskById(orphan6TaskId);
        task7 = client.getTaskById(orphan7TaskId);

        assertNull(task1.isOrphan());
        assertNull(task1.getParentId());
        assertEquals(orphan1TaskId, task1.getPrimaryId());
        assertNull(task1.getParentsPath());
        assertEquals(CTX + "1", task1.getCtx().get(CTX +"1"));

        assertTrue(task2.isOrphan() == null || !task2.isOrphan());
        assertEquals(orphan1TaskId, task2.getParentId());
        assertEquals(orphan1TaskId, task2.getPrimaryId());
        assertEquals(1, task2.getParentsPath().size());
        assertEquals(ORPHAN +"1", task2.getParentsPath().get(0));
        assertEquals(CTX + "1", task2.getCtx().get(CTX +"1"));
        assertEquals(CTX + "2", task2.getCtx().get(CTX +"2"));

        assertFalse(task3.isOrphan());
        assertEquals(orphan2TaskId, task3.getParentId());
        assertEquals(orphan1TaskId, task3.getPrimaryId());
        assertEquals(2, task3.getParentsPath().size());
        assertEquals(ORPHAN +"1", task3.getParentsPath().get(0));
        assertEquals(ORPHAN +"2", task3.getParentsPath().get(1));
        assertEquals(CTX + "1", task3.getCtx().get(CTX +"1"));
        assertEquals(CTX + "2", task3.getCtx().get(CTX +"2"));
        assertEquals(CTX + "3", task3.getCtx().get(CTX +"3"));

        assertFalse(task4.isOrphan());
        assertEquals(orphan3TaskId, task4.getParentId());
        assertEquals(orphan1TaskId, task4.getPrimaryId());
        assertEquals(3, task4.getParentsPath().size());
        assertEquals(ORPHAN +"1", task4.getParentsPath().get(0));
        assertEquals(ORPHAN +"2", task4.getParentsPath().get(1));
        assertEquals(ORPHAN +"3", task4.getParentsPath().get(2));
        assertEquals(CTX + "1", task4.getCtx().get(CTX +"1"));
        assertEquals(CTX + "2", task4.getCtx().get(CTX +"2"));
        assertEquals(CTX + "3", task4.getCtx().get(CTX +"3"));
        assertEquals(CTX + "4", task4.getCtx().get(CTX +"4"));

        assertFalse(task5.isOrphan());
        assertEquals(orphan4TaskId, task5.getParentId());
        assertEquals(orphan1TaskId, task5.getPrimaryId());
        assertEquals(4, task5.getParentsPath().size());
        assertEquals(ORPHAN +"1", task5.getParentsPath().get(0));
        assertEquals(ORPHAN +"2", task5.getParentsPath().get(1));
        assertEquals(ORPHAN +"3", task5.getParentsPath().get(2));
        assertEquals(ORPHAN +"4", task5.getParentsPath().get(3));
        assertEquals(CTX + "1", task5.getCtx().get(CTX +"1"));
        assertEquals(CTX + "2", task5.getCtx().get(CTX +"2"));
        assertEquals(CTX + "3", task5.getCtx().get(CTX +"3"));
        assertEquals(CTX + "4", task5.getCtx().get(CTX +"4"));
        assertEquals(CTX + "5", task5.getCtx().get(CTX +"5"));

        assertFalse(task6.isOrphan());
        assertEquals(orphan5TaskId, task6.getParentId());
        assertEquals(orphan1TaskId, task6.getPrimaryId());
        assertEquals(5, task6.getParentsPath().size());
        assertEquals(ORPHAN +"1", task6.getParentsPath().get(0));
        assertEquals(ORPHAN +"2", task6.getParentsPath().get(1));
        assertEquals(ORPHAN +"3", task6.getParentsPath().get(2));
        assertEquals(ORPHAN +"4", task6.getParentsPath().get(3));
        assertEquals(ORPHAN +"5", task6.getParentsPath().get(4));
        assertEquals(CTX + "1", task6.getCtx().get(CTX +"1"));
        assertEquals(CTX + "2", task6.getCtx().get(CTX +"2"));
        assertEquals(CTX + "3", task6.getCtx().get(CTX +"3"));
        assertEquals(CTX + "4", task6.getCtx().get(CTX +"4"));
        assertEquals(CTX + "5", task6.getCtx().get(CTX +"5"));
        assertEquals(CTX + "6", task6.getCtx().get(CTX +"6"));

        assertFalse(task7.isOrphan());
        assertEquals(orphan6TaskId, task7.getParentId());
        assertEquals(orphan1TaskId, task7.getPrimaryId());
        assertEquals(6, task7.getParentsPath().size());
        assertEquals(ORPHAN +"1", task7.getParentsPath().get(0));
        assertEquals(ORPHAN +"2", task7.getParentsPath().get(1));
        assertEquals(ORPHAN +"3", task7.getParentsPath().get(2));
        assertEquals(ORPHAN +"4", task7.getParentsPath().get(3));
        assertEquals(ORPHAN +"5", task7.getParentsPath().get(4));
        assertEquals(ORPHAN +"6", task7.getParentsPath().get(5));
        assertEquals(CTX + "1", task7.getCtx().get(CTX +"1"));
        assertEquals(CTX + "2", task7.getCtx().get(CTX +"2"));
        assertEquals(CTX + "3", task7.getCtx().get(CTX +"3"));
        assertEquals(CTX + "4", task7.getCtx().get(CTX +"4"));
        assertEquals(CTX + "5", task7.getCtx().get(CTX +"5"));
        assertEquals(CTX + "6", task7.getCtx().get(CTX +"6"));
        assertEquals(CTX + "7", task7.getCtx().get(CTX +"7"));
    }

    public void testOrphanWithAdoptionDifferentBatches() {
        String orphan1TaskId = Event.generateTaskId(ORPHAN +"1");
        TimberLoggerAdvanced.start(orphan1TaskId, ORPHAN +"1", null, LogParams.create().context(CTX + "1", CTX + "1"));
        TimberLogTest.waitForEvents(orphan1TaskId, TaskStatus.UNTERMINATED);
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

        TimberLogTest.waitForEvents(orphan1TaskId, TaskStatus.SUCCESS);
        TimberLogTest.waitForEvents(orphan3TaskId, TaskStatus.SUCCESS);
        TimberLogTest.waitForEvents(orphan5TaskId, TaskStatus.SUCCESS);
        TimberLogTest.waitForEvents(orphan7TaskId, TaskStatus.SUCCESS);


        Task task1 = client.getTaskById(orphan1TaskId);
        Task task3 = client.getTaskById(orphan3TaskId);
        Task task5 = client.getTaskById(orphan5TaskId);
        Task task7 = client.getTaskById(orphan7TaskId);

        assertNull(task1.isOrphan());
        assertNull(task1.getParentId());
        assertEquals(orphan1TaskId, task1.getPrimaryId());
        assertNull(task1.getParentsPath());
        assertEquals(CTX + "1", task1.getCtx().get(CTX +"1"));

        assertTrue(task3.isOrphan());
        assertEquals(orphan2TaskId, task3.getParentId());
        assertNull(task3.getPrimaryId());
        assertEquals(CTX + "3", task3.getCtx().get(CTX +"3"));

        assertTrue(task5.isOrphan());
        assertEquals(orphan4TaskId, task5.getParentId());
        assertNull(task5.getPrimaryId());
        assertEquals(CTX + "5", task5.getCtx().get(CTX +"5"));

        assertTrue(task7.isOrphan());
        assertEquals(orphan6TaskId, task7.getParentId());
        assertNull(task7.getPrimaryId());
        assertEquals(CTX + "7", task7.getCtx().get(CTX +"7"));


        TimberLoggerAdvanced.start(orphan2TaskId, ORPHAN +"2", orphan1TaskId, LogParams.create().context(CTX + "2", CTX + "2"));
        TimberLoggerAdvanced.success(orphan2TaskId);

        TimberLogTest.waitForEvents(orphan2TaskId, TaskStatus.SUCCESS);

        TimberLoggerAdvanced.start(orphan4TaskId, ORPHAN +"4", orphan3TaskId, LogParams.create().context(CTX + "4", CTX + "4"));
        TimberLoggerAdvanced.success(orphan4TaskId);

        TimberLoggerAdvanced.start(orphan6TaskId, ORPHAN +"6", orphan5TaskId, LogParams.create().context(CTX + "6", CTX + "6"));
        TimberLoggerAdvanced.success(orphan6TaskId);

        TimberLogTest.waitForEvents(orphan4TaskId, TaskStatus.SUCCESS);
        TimberLogTest.waitForEvents(orphan6TaskId, TaskStatus.SUCCESS);

        task1 = client.getTaskById(orphan1TaskId);
        Task task2 = client.getTaskById(orphan2TaskId);
        task3 = client.getTaskById(orphan3TaskId);
        Task task4 = client.getTaskById(orphan4TaskId);
        task5 = client.getTaskById(orphan5TaskId);
        Task task6 = client.getTaskById(orphan6TaskId);
        task7 = client.getTaskById(orphan7TaskId);

        assertNull(task1.isOrphan());
        assertNull(task1.getParentId());
        assertEquals(orphan1TaskId, task1.getPrimaryId());
        assertNull(task1.getParentsPath());
        assertEquals(CTX + "1", task1.getCtx().get(CTX +"1"));

        assertTrue(task2.isOrphan() == null || !task2.isOrphan());
        assertEquals(orphan1TaskId, task2.getParentId());
        assertEquals(orphan1TaskId, task2.getPrimaryId());
        assertEquals(1, task2.getParentsPath().size());
        assertEquals(ORPHAN +"1", task2.getParentsPath().get(0));
        assertEquals(CTX + "1", task2.getCtx().get(CTX +"1"));
        assertEquals(CTX + "2", task2.getCtx().get(CTX +"2"));

        assertFalse(task3.isOrphan());
        assertEquals(orphan2TaskId, task3.getParentId());
        assertEquals(orphan1TaskId, task3.getPrimaryId());
        assertEquals(2, task3.getParentsPath().size());
        assertEquals(ORPHAN +"1", task3.getParentsPath().get(0));
        assertEquals(ORPHAN +"2", task3.getParentsPath().get(1));
        assertEquals(CTX + "1", task3.getCtx().get(CTX +"1"));
        assertEquals(CTX + "2", task3.getCtx().get(CTX +"2"));
        assertEquals(CTX + "3", task3.getCtx().get(CTX +"3"));

        assertFalse(task4.isOrphan());
        assertEquals(orphan3TaskId, task4.getParentId());
        assertEquals(orphan1TaskId, task4.getPrimaryId());
        assertEquals(3, task4.getParentsPath().size());
        assertEquals(ORPHAN +"1", task4.getParentsPath().get(0));
        assertEquals(ORPHAN +"2", task4.getParentsPath().get(1));
        assertEquals(ORPHAN +"3", task4.getParentsPath().get(2));
        assertEquals(CTX + "1", task4.getCtx().get(CTX +"1"));
        assertEquals(CTX + "2", task4.getCtx().get(CTX +"2"));
        assertEquals(CTX + "3", task4.getCtx().get(CTX +"3"));
        assertEquals(CTX + "4", task4.getCtx().get(CTX +"4"));

        assertFalse(task5.isOrphan());
        assertEquals(orphan4TaskId, task5.getParentId());
        assertEquals(orphan1TaskId, task5.getPrimaryId());
        assertEquals(4, task5.getParentsPath().size());
        assertEquals(ORPHAN +"1", task5.getParentsPath().get(0));
        assertEquals(ORPHAN +"2", task5.getParentsPath().get(1));
        assertEquals(ORPHAN +"3", task5.getParentsPath().get(2));
        assertEquals(ORPHAN +"4", task5.getParentsPath().get(3));
        assertEquals(CTX + "1", task5.getCtx().get(CTX +"1"));
        assertEquals(CTX + "2", task5.getCtx().get(CTX +"2"));
        assertEquals(CTX + "3", task5.getCtx().get(CTX +"3"));
        assertEquals(CTX + "4", task5.getCtx().get(CTX +"4"));
        assertEquals(CTX + "5", task5.getCtx().get(CTX +"5"));

        assertFalse(task6.isOrphan());
        assertEquals(orphan5TaskId, task6.getParentId());
        assertEquals(orphan1TaskId, task6.getPrimaryId());
        assertEquals(5, task6.getParentsPath().size());
        assertEquals(ORPHAN +"1", task6.getParentsPath().get(0));
        assertEquals(ORPHAN +"2", task6.getParentsPath().get(1));
        assertEquals(ORPHAN +"3", task6.getParentsPath().get(2));
        assertEquals(ORPHAN +"4", task6.getParentsPath().get(3));
        assertEquals(ORPHAN +"5", task6.getParentsPath().get(4));
        assertEquals(CTX + "1", task6.getCtx().get(CTX +"1"));
        assertEquals(CTX + "2", task6.getCtx().get(CTX +"2"));
        assertEquals(CTX + "3", task6.getCtx().get(CTX +"3"));
        assertEquals(CTX + "4", task6.getCtx().get(CTX +"4"));
        assertEquals(CTX + "5", task6.getCtx().get(CTX +"5"));
        assertEquals(CTX + "6", task6.getCtx().get(CTX +"6"));

        assertFalse(task7.isOrphan());
        assertEquals(orphan6TaskId, task7.getParentId());
        assertEquals(orphan1TaskId, task7.getPrimaryId());
        assertEquals(6, task7.getParentsPath().size());
        assertEquals(ORPHAN +"1", task7.getParentsPath().get(0));
        assertEquals(ORPHAN +"2", task7.getParentsPath().get(1));
        assertEquals(ORPHAN +"3", task7.getParentsPath().get(2));
        assertEquals(ORPHAN +"4", task7.getParentsPath().get(3));
        assertEquals(ORPHAN +"5", task7.getParentsPath().get(4));
        assertEquals(ORPHAN +"6", task7.getParentsPath().get(5));
        assertEquals(CTX + "1", task7.getCtx().get(CTX +"1"));
        assertEquals(CTX + "2", task7.getCtx().get(CTX +"2"));
        assertEquals(CTX + "3", task7.getCtx().get(CTX +"3"));
        assertEquals(CTX + "4", task7.getCtx().get(CTX +"4"));
        assertEquals(CTX + "5", task7.getCtx().get(CTX +"5"));
        assertEquals(CTX + "6", task7.getCtx().get(CTX +"6"));
        assertEquals(CTX + "7", task7.getCtx().get(CTX +"7"));
    }
}

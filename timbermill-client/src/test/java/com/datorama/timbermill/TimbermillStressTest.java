package com.datorama.timbermill;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datorama.timbermill.annotation.TimberLog;
import com.datorama.timbermill.pipe.TimbermillServerOutputPipe;
import com.datorama.timbermill.unit.Event;
import com.datorama.timbermill.unit.LogParams;
import com.datorama.timbermill.unit.Task;
import com.google.common.collect.Lists;

import static org.junit.Assert.*;

public class TimbermillStressTest extends TimberLogTest{

    private static final int NUM_OF_TASKS = 10000;
    private static final int NUM_OF_THREADS = 10;
    private static final int NUMBER_OF_PARENTS = 30;
    private static final String CTX = "ctx";
    private static ExecutorService executorService;

    @BeforeClass
    public static void init() {
        executorService = Executors.newFixedThreadPool(NUM_OF_THREADS);
        TimbermillServerOutputPipe pipe = new TimbermillServerOutputPipe.Builder().timbermillServerUrl("http://localhost:8484/events").build();
        TimberLogger.bootstrap(pipe, TEST);
    }

    @Test
    public void simpleStressTest() throws ExecutionException, InterruptedException {
        Runnable simpleRunnable = () -> {
            String taskId = runSimpleStress();
            waitForEvent(taskId, Task.TaskStatus.SUCCESS);
        };
        runInParallel(simpleRunnable);
    }

    @Test
    public void advanceStressTest() throws ExecutionException, InterruptedException {
        Runnable advancedRunnable = () -> {
            List<String> tasksIds = createAdvanceTasks();
            String taskId = tasksIds.get(NUM_OF_TASKS - 1);
            waitForEvent(taskId, Task.TaskStatus.SUCCESS);
            String childTaskId = createChildrenTasks(tasksIds);
            waitForEvent(childTaskId, Task.TaskStatus.SUCCESS);
            Task childTask = client.getTaskById(childTaskId);
            assertEquals(childTask.getCtx().get(CTX), childTask.getParentId());
        };
        runInParallel(advancedRunnable);
    }

    @Test
    public void orphansStressTest() throws ExecutionException, InterruptedException {
        Runnable orphansRunnable = () -> {
            Pair<String, String> parentOrphan = createOrphansStress();
            String parentTaskId = parentOrphan.getLeft();
            String orphanTaskId = parentOrphan.getRight();
            waitForEvent(parentTaskId, Task.TaskStatus.SUCCESS);
            waitForEvent(orphanTaskId, Task.TaskStatus.SUCCESS);
            Task orphanTask = client.getTaskById(orphanTaskId);
            assertEquals(orphanTask.getCtx().get(CTX), orphanTask.getPrimaryId());
        };
        runInParallel(orphansRunnable);
    }

    @Test
    public void stringOfOrphansStressTest() throws ExecutionException, InterruptedException {
        Runnable orphansStringsRunnable = () -> {
            Pair<String, String> primaryOrphan = createStringOfOrphansStress();
            String primaryTaskId = primaryOrphan.getLeft();
            String orphanTaskId = primaryOrphan.getRight();
            waitForEvent(primaryTaskId, Task.TaskStatus.SUCCESS);
            waitForEvent(orphanTaskId, Task.TaskStatus.SUCCESS);

            Task orphanTask = client.getTaskById(orphanTaskId);

            assertFalse(orphanTask.isOrphan());
            assertNotNull(orphanTask.getParentId());
            assertEquals(primaryTaskId, orphanTask.getPrimaryId());
            assertEquals(NUMBER_OF_PARENTS - 1, orphanTask.getParentsPath().size());

            assertEquals(CTX, orphanTask.getCtx().get(CTX));
        };
        runInParallel(orphansStringsRunnable);
    }

    private Pair<String, String> createStringOfOrphansStress() {
        String parentTaskId = null;
        String taskId = null;
        for (int i = 0; i < NUMBER_OF_PARENTS; i++) {
            if (parentTaskId == null) {
                parentTaskId = Event.generateTaskId("orphan_string_stress");
                taskId = TimberLoggerAdvanced.start("orphan_string_stress", parentTaskId);
                waitForEvent(taskId, Task.TaskStatus.UNTERMINATED);
                TimberLoggerAdvanced.success(taskId);
            }
            else{
                if(i + 1 == NUMBER_OF_PARENTS){
                    TimberLoggerAdvanced.start(parentTaskId, "orphan_string_stress",  null, LogParams.create().context(CTX, CTX));
                    TimberLoggerAdvanced.success(parentTaskId);
                }
                else{
                    String newParentTaskId = Event.generateTaskId("orphan_string_stress");
                    TimberLoggerAdvanced.start(parentTaskId, "orphan_string_stress",  newParentTaskId, null);
                    TimberLoggerAdvanced.success(parentTaskId);
                    parentTaskId = newParentTaskId;
                }
            }
        }
        return Pair.of(parentTaskId, taskId);
    }

    private Pair<String, String> createOrphansStress() {
        String orphan = null;
        String parent = null;
        List<String> parentTasks = Lists.newLinkedList();
        for (int i = 0; i < NUM_OF_TASKS; i++) {
            String parentTaskId = Event.generateTaskId("parent_stress");
            orphan = TimberLogger.start("orphan_stress", parentTaskId, null);
            TimberLogger.success();
            parentTasks.add(parentTaskId);
        }
        for (String parentTaskId : parentTasks) {
            TimberLoggerAdvanced.start(parentTaskId, "parent_stress", null, LogParams.create().context(CTX, parentTaskId));
            TimberLoggerAdvanced.success(parentTaskId);
            parent = parentTaskId;
        }
        return Pair.of(parent, orphan);
    }

    private String createChildrenTasks(List<String> tasksIds) {
        String childTaskId = null;
        for (String tasksId : tasksIds) {
            childTaskId = TimberLoggerAdvanced.start("child_stress", tasksId);
            TimberLoggerAdvanced.success(childTaskId);
        }
        return childTaskId;
    }

    private void runInParallel(Runnable task) throws InterruptedException, ExecutionException {
        List<Future<?>> futures = Lists.newArrayList();
        for (int i = 0; i < NUM_OF_THREADS; i++) {
            Future<?> future = executorService.submit(task);
            futures.add(future);
        }
        for (Future future : futures) {
                future.get();
        }
    }

    private List<String> createAdvanceTasks() {
        List<String> retList = new ArrayList<>();
        for (int i = 0; i < NUM_OF_TASKS; i++) {
            String taskId = TimberLoggerAdvanced.start("advanced_stress");
            TimberLoggerAdvanced.logParams(taskId, LogParams.create().context(CTX, taskId));
            TimberLoggerAdvanced.success(taskId);
            retList.add(taskId);
        }
        return retList;
    }

    private String runSimpleStress() {
        String retStr = null;
        for (int i = 0; i < NUM_OF_TASKS; i++) {
            retStr = simpleStressLog();
        }
        return retStr;
    }

    @TimberLog(name = "simple_stress")
    private String simpleStressLog() {
        String currentTaskId = TimberLogger.getCurrentTaskId();
        TimberLogger.logContext(CTX, currentTaskId);
        return currentTaskId;
    }
}

package com.datorama.timbermill;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.timbermill.annotation.TimberLog;
import com.datorama.timbermill.common.Constants;
import com.datorama.timbermill.pipe.TimbermillServerOutputPipe;
import com.datorama.timbermill.pipe.TimbermillServerOutputPipeBuilder;
import com.datorama.timbermill.unit.Event;
import com.datorama.timbermill.unit.LogParams;
import com.datorama.timbermill.unit.Task;
import com.google.common.collect.Lists;

import static org.junit.Assert.*;

public class TimbermillStressTest extends TimberLogTest{

    private static final Logger LOG = LoggerFactory.getLogger(TimbermillStressTest.class);
    private static final String CTX = "ctx";
    private static ExecutorService executorService;
    private static String env;
    private static int numOfParents = 30;
    private static int numOfThreads = 10;
    private static int numOfTasks = 10;

    @BeforeClass
    public static void init() {
        try {
            numOfParents = Integer.parseInt(System.getenv("NUM_OF_PARENTS"));
            numOfThreads = Integer.parseInt(System.getenv("NUM_OF_THREADS"));
            numOfTasks = Integer.parseInt(System.getenv("NUM_OF_TASKS"));
        } catch (Throwable ignored){}

        LOG.info("numOfParents = {}", numOfParents);
        LOG.info("numOfThreads = {}", numOfThreads);
        LOG.info("numOfTasks = {}", numOfTasks);
        String timbermillUrl = System.getenv("TIMBERMILL_URL");
        if (StringUtils.isEmpty(timbermillUrl)){
            timbermillUrl = Constants.DEFAULT_TIMBERMILL_URL;
        }

        String elasticUrl = System.getenv("ELASTICSEARCH_URL");
        if (StringUtils.isEmpty(elasticUrl)){
            elasticUrl = Constants.DEFAULT_ELASTICSEARCH_URL;
        }

        String awsRegion = System.getenv("ELASTICSEARCH_AWS_REGION");
        if (StringUtils.isEmpty(awsRegion)){
            awsRegion = null;
        }
        client = new ElasticsearchClient(elasticUrl, 1000, 1, awsRegion, null, null,
                7, 100, 1000000000);
        executorService = Executors.newFixedThreadPool(numOfThreads);
        TimbermillServerOutputPipe pipe = new TimbermillServerOutputPipeBuilder().timbermillServerUrl(timbermillUrl).build();
        env = TEST + System.currentTimeMillis();
        TimberLogger.bootstrap(pipe, env);
    }

    @AfterClass
    public static void tearDown() {
        TimberLogger.exit();
        executorService.shutdown();
    }


    @Test
    public void simpleStressTest() throws ExecutionException, InterruptedException, IOException {
        LOG.info("Running test {}", "simpleStressTest");
        Runnable simpleRunnable = () -> {
            String taskId = runSimpleStress();
            waitForTask(taskId, Task.TaskStatus.SUCCESS);
        };
        runInParallel(simpleRunnable);
        assertEquals( numOfTasks * numOfThreads, client.countByName("simple_stress", env));
    }

    @Test
    public void advanceStressTest() throws ExecutionException, InterruptedException, IOException {
        LOG.info("Running test {}", "advanceStressTest");
        Runnable advancedRunnable = () -> {
            List<String> tasksIds = createAdvanceTasks();
            String taskId = tasksIds.get(numOfTasks - 1);
            waitForTask(taskId, Task.TaskStatus.SUCCESS);
            String childTaskId = createChildrenTasks(tasksIds);
            waitForTask(childTaskId, Task.TaskStatus.SUCCESS);
            Task childTask = client.getTaskById(childTaskId);
            assertEquals(childTask.getCtx().get(CTX), childTask.getParentId());
        };
        runInParallel(advancedRunnable);
        assertEquals( numOfTasks * numOfThreads, client.countByName("advanced_stress", env));
    }

    @Test
    public void orphansStressTest() throws ExecutionException, InterruptedException, IOException {
        LOG.info("Running test {}", "orphansStressTest");
        Runnable orphansRunnable = () -> {
            Pair<String, String> parentOrphan = createOrphansStress();
            String parentTaskId = parentOrphan.getLeft();
            String orphanTaskId = parentOrphan.getRight();
            waitForTask(parentTaskId, Task.TaskStatus.SUCCESS);
            waitForTask(orphanTaskId, Task.TaskStatus.SUCCESS);
            Task orphanTask = client.getTaskById(orphanTaskId);
            assertEquals(orphanTask.getCtx().get(CTX), orphanTask.getPrimaryId());
        };
        runInParallel(orphansRunnable);
        assertEquals( numOfTasks * numOfThreads, client.countByName("parent_stress", env));
        assertEquals( numOfTasks * numOfThreads, client.countByName("orphan_stress", env));
    }

    @Test
    public void stringOfOrphansStressTest() throws ExecutionException, InterruptedException, IOException {
        LOG.info("Running test {}", "stringOfOrphansStressTest");
        Runnable orphansStringsRunnable = () -> {
            Pair<String, String> primaryOrphan = createStringOfOrphansStress();
            String primaryTaskId = primaryOrphan.getLeft();
            String orphanTaskId = primaryOrphan.getRight();
            waitForTask(primaryTaskId, Task.TaskStatus.SUCCESS);
            waitForTask(orphanTaskId, Task.TaskStatus.SUCCESS);

            Task orphanTask = client.getTaskById(orphanTaskId);

            assertFalse(orphanTask.isOrphan());
            assertNotNull(orphanTask.getParentId());
            assertEquals(primaryTaskId, orphanTask.getPrimaryId());
            assertEquals(numOfParents - 1, orphanTask.getParentsPath().size());

            assertEquals(CTX, orphanTask.getCtx().get(CTX));
        };
        runInParallel(orphansStringsRunnable);
        assertEquals( numOfParents * numOfThreads, client.countByName("orphan_string_stress", env));
    }

    private Pair<String, String> createStringOfOrphansStress() {
        String parentTaskId = null;
        String taskId = null;
        for (int i = 0; i < numOfParents; i++) {
            if (parentTaskId == null) {
                parentTaskId = Event.generateTaskId("orphan_string_stress");
                taskId = TimberLoggerAdvanced.start("orphan_string_stress", parentTaskId);
                waitForTask(taskId, Task.TaskStatus.UNTERMINATED);
                TimberLoggerAdvanced.success(taskId);
            }
            else{
                if(i + 1 == numOfParents){
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
        for (int i = 0; i < numOfTasks; i++) {
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
        for (int i = 0; i < numOfThreads; i++) {
            Future<?> future = executorService.submit(task);
            futures.add(future);
        }
        for (Future future : futures) {
                future.get();
        }
    }

    private List<String> createAdvanceTasks() {
        List<String> retList = new ArrayList<>();
        for (int i = 0; i < numOfTasks; i++) {
            String taskId = TimberLoggerAdvanced.start("advanced_stress");
            TimberLoggerAdvanced.logParams(taskId, LogParams.create().context(CTX, taskId));
            TimberLoggerAdvanced.success(taskId);
            retList.add(taskId);
        }
        return retList;
    }

    private String runSimpleStress() {
        String retStr = null;
        for (int i = 0; i < numOfTasks; i++) {
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

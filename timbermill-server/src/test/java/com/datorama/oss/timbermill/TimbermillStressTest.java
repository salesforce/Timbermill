package com.datorama.oss.timbermill;

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
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.oss.timbermill.annotation.TimberLogTask;
import com.datorama.oss.timbermill.pipe.TimbermillServerOutputPipe;
import com.datorama.oss.timbermill.pipe.TimbermillServerOutputPipeBuilder;
import com.datorama.oss.timbermill.unit.Event;
import com.datorama.oss.timbermill.unit.LogParams;
import com.datorama.oss.timbermill.unit.Task;
import com.datorama.oss.timbermill.unit.TaskStatus;
import com.google.common.collect.Lists;

import static com.datorama.oss.timbermill.common.Constants.DEFAULT_ELASTICSEARCH_URL;
import static com.datorama.oss.timbermill.common.Constants.DEFAULT_TIMBERMILL_URL;
import static org.junit.Assert.*;

@Ignore
public class TimbermillStressTest extends TimberLogTest{

    private static final Logger LOG = LoggerFactory.getLogger(TimbermillStressTest.class);
    private static final String CTX = "ctx";
    private static ExecutorService executorService;
    private static String env;
    private static int numOfParents = 10;
    private static int numOfThreads = 10;
    private static int numOfTasks = 100000;
    private static int maxBufferSize = 20000000;

    @BeforeClass
    public static void init() {
        try {
            numOfParents = Integer.parseInt(System.getenv("NUM_OF_PARENTS"));
            numOfThreads = Integer.parseInt(System.getenv("NUM_OF_THREADS"));
            numOfTasks = Integer.parseInt(System.getenv("NUM_OF_TASKS"));
            maxBufferSize = Integer.parseInt(System.getenv("MAX_BUFFER_SIZE"));
        } catch (Throwable ignored){}

        LOG.info("numOfParents = {}", numOfParents);
        LOG.info("numOfThreads = {}", numOfThreads);
        LOG.info("numOfTasks = {}", numOfTasks);
        LOG.info("maxBufferSize = {}", maxBufferSize);
        String timbermillUrl = System.getenv("TIMBERMILL_URL");
        if (StringUtils.isEmpty(timbermillUrl)){
            timbermillUrl = DEFAULT_TIMBERMILL_URL;
        }

        String elasticUrl = System.getenv("ELASTICSEARCH_URL");
        if (StringUtils.isEmpty(elasticUrl)){
            elasticUrl = DEFAULT_ELASTICSEARCH_URL;
        }

        String awsRegion = System.getenv("ELASTICSEARCH_AWS_REGION");
        if (StringUtils.isEmpty(awsRegion)){
            awsRegion = null;
        }
        client = new ElasticsearchClient(elasticUrl, 1000, 1, awsRegion, null, null,
                7, 100, 1000000000, 3, 3, 1000,null ,1, 1, 4000);
        executorService = Executors.newFixedThreadPool(numOfThreads);
        TimbermillServerOutputPipe pipe = new TimbermillServerOutputPipeBuilder().timbermillServerUrl(timbermillUrl).maxBufferSize(maxBufferSize).build();
        env = TEST + System.currentTimeMillis();
        TimberLogger.bootstrap(pipe, env);
    }

    @AfterClass
    public static void tearDown() {
        TimberLogger.exit();
        executorService.shutdown();
    }


    @Ignore
    @Test
    public void simpleStressTest() throws ExecutionException, InterruptedException, IOException {
        LOG.info("Running test {}", "simpleStressTest");
        Runnable simpleRunnable = () -> {
            String taskId = runSimpleStress();
            waitForTask(taskId, TaskStatus.SUCCESS, client);
        };
        runInParallel(simpleRunnable);
        assertEquals( numOfTasks * numOfThreads, client.countByName("simple_stress", env));
    }

    @Ignore
    @Test
    public void advanceStressTest() throws ExecutionException, InterruptedException, IOException {
        LOG.info("Running test {}", "advanceStressTest");
        Runnable advancedRunnable = () -> {
            List<String> tasksIds = createAdvanceTasks();
            String taskId = tasksIds.get(numOfTasks - 1);
            waitForTask(taskId, TaskStatus.SUCCESS, client);
            String childTaskId = createChildrenTasks(tasksIds);
            waitForTask(childTaskId, TaskStatus.SUCCESS, client);
            Task childTask = client.getTaskById(childTaskId);
            assertEquals(childTask.getCtx().get(CTX), childTask.getParentId());
        };
        runInParallel(advancedRunnable);
        assertEquals( numOfTasks * numOfThreads, client.countByName("advanced_stress", env));
    }

    @Ignore
    @Test
    public void orphansStressTest() throws ExecutionException, InterruptedException, IOException {
        LOG.info("Running test {}", "orphansStressTest");
        Runnable orphansRunnable = () -> {
            Pair<String, String> parentOrphan = createOrphansStress();
            String parentTaskId = parentOrphan.getLeft();
            String orphanTaskId = parentOrphan.getRight();
            waitForTask(parentTaskId, TaskStatus.SUCCESS, client);
            waitForTask(orphanTaskId, TaskStatus.SUCCESS, client);
            Task orphanTask = client.getTaskById(orphanTaskId);
            assertEquals(orphanTask.getCtx().get(CTX), orphanTask.getPrimaryId());
        };
        runInParallel(orphansRunnable);
        assertEquals( numOfTasks * numOfThreads, client.countByName("parent_stress", env));
        assertEquals( numOfTasks * numOfThreads, client.countByName("orphan_stress", env));
    }

    @Ignore
    @Test
    public void stringOfOrphansStressTest() throws ExecutionException, InterruptedException, IOException {
        LOG.info("Running test {}", "stringOfOrphansStressTest");
        Runnable orphansStringsRunnable = () -> {
            Pair<String, String> primaryOrphan = createStringOfOrphansStress();
            String primaryTaskId = primaryOrphan.getLeft();
            String orphanTaskId = primaryOrphan.getRight();
            waitForTask(primaryTaskId, TaskStatus.SUCCESS, client);
            waitForTask(orphanTaskId, TaskStatus.SUCCESS, client);

            Task orphanTask = client.getTaskById(orphanTaskId);

            TimberLogTest.assertNotOrphan(orphanTask);
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
                TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text1", "TEXT"));
                TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text2", "TEXT"));
                TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text3", "TEXT"));
                TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text4", "TEXT"));
                TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text5", "TEXT"));
                TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text6", "TEXT"));
                TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text7", "TEXT"));
                TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text8", "TEXT"));
                TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text9", "TEXT"));
                TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text10", "TEXT"));
                TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text11", "TEXT"));
                TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text12", "TEXT"));
                TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text13", "TEXT"));
                TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text14", "TEXT"));
                TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text15", "TEXT"));
                TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text16", "TEXT"));
                TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text17", "TEXT"));
                TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text18", "TEXT"));
                TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text19", "TEXT"));
                TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text20", "TEXT"));
                waitForTask(taskId, TaskStatus.UNTERMINATED, client);
                TimberLoggerAdvanced.success(taskId);
            }
            else{
                if(i + 1 == numOfParents){
                    TimberLoggerAdvanced.start(parentTaskId, "orphan_string_stress",  null, LogParams.create().context(CTX, CTX));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text1", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text2", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text3", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text4", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text5", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text6", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text7", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text8", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text9", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text10", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text11", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text12", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text13", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text14", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text15", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text16", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text17", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text18", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text19", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text20", "TEXT"));
                    TimberLoggerAdvanced.success(parentTaskId);
                }
                else{
                    String newParentTaskId = Event.generateTaskId("orphan_string_stress");
                    TimberLoggerAdvanced.start(parentTaskId, "orphan_string_stress",  newParentTaskId, null);
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text1", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text2", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text3", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text4", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text5", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text6", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text7", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text8", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text9", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text10", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text11", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text12", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text13", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text14", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text15", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text16", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text17", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text18", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text19", "TEXT"));
                    TimberLoggerAdvanced.logParams(parentTaskId, LogParams.create().text("text20", "TEXT"));
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
            TimberLogger.logText("text1", "Text");
            TimberLogger.logText("text2", "Text");
            TimberLogger.logText("text3", "Text");
            TimberLogger.logText("text4", "Text");
            TimberLogger.logText("text5", "Text");
            TimberLogger.logText("text6", "Text");
            TimberLogger.logText("text7", "Text");
            TimberLogger.logText("text8", "Text");
            TimberLogger.logText("text9", "Text");
            TimberLogger.logText("text10", "Text");
            TimberLogger.logText("text11", "Text");
            TimberLogger.logText("text12", "Text");
            TimberLogger.logText("text13", "Text");
            TimberLogger.logText("text14", "Text");
            TimberLogger.logText("text15", "Text");
            TimberLogger.logText("text16", "Text");
            TimberLogger.logText("text17", "Text");
            TimberLogger.logText("text18", "Text");
            TimberLogger.logText("text19", "Text");
            TimberLogger.logText("text20", "Text");
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
            TimberLoggerAdvanced.logParams(childTaskId, LogParams.create().text("text1", "TEXT"));
            TimberLoggerAdvanced.logParams(childTaskId, LogParams.create().text("text2", "TEXT"));
            TimberLoggerAdvanced.logParams(childTaskId, LogParams.create().text("text3", "TEXT"));
            TimberLoggerAdvanced.logParams(childTaskId, LogParams.create().text("text4", "TEXT"));
            TimberLoggerAdvanced.logParams(childTaskId, LogParams.create().text("text5", "TEXT"));
            TimberLoggerAdvanced.logParams(childTaskId, LogParams.create().text("text6", "TEXT"));
            TimberLoggerAdvanced.logParams(childTaskId, LogParams.create().text("text7", "TEXT"));
            TimberLoggerAdvanced.logParams(childTaskId, LogParams.create().text("text8", "TEXT"));
            TimberLoggerAdvanced.logParams(childTaskId, LogParams.create().text("text9", "TEXT"));
            TimberLoggerAdvanced.logParams(childTaskId, LogParams.create().text("text10", "TEXT"));
            TimberLoggerAdvanced.logParams(childTaskId, LogParams.create().text("text11", "TEXT"));
            TimberLoggerAdvanced.logParams(childTaskId, LogParams.create().text("text12", "TEXT"));
            TimberLoggerAdvanced.logParams(childTaskId, LogParams.create().text("text13", "TEXT"));
            TimberLoggerAdvanced.logParams(childTaskId, LogParams.create().text("text14", "TEXT"));
            TimberLoggerAdvanced.logParams(childTaskId, LogParams.create().text("text15", "TEXT"));
            TimberLoggerAdvanced.logParams(childTaskId, LogParams.create().text("text16", "TEXT"));
            TimberLoggerAdvanced.logParams(childTaskId, LogParams.create().text("text17", "TEXT"));
            TimberLoggerAdvanced.logParams(childTaskId, LogParams.create().text("text18", "TEXT"));
            TimberLoggerAdvanced.logParams(childTaskId, LogParams.create().text("text19", "TEXT"));
            TimberLoggerAdvanced.logParams(childTaskId, LogParams.create().text("text20", "TEXT"));
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
            TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text1", "TEXT"));
            TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text2", "TEXT"));
            TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text3", "TEXT"));
            TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text4", "TEXT"));
            TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text5", "TEXT"));
            TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text6", "TEXT"));
            TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text7", "TEXT"));
            TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text8", "TEXT"));
            TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text9", "TEXT"));
            TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text10", "TEXT"));
            TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text11", "TEXT"));
            TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text12", "TEXT"));
            TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text13", "TEXT"));
            TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text14", "TEXT"));
            TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text15", "TEXT"));
            TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text16", "TEXT"));
            TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text17", "TEXT"));
            TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text18", "TEXT"));
            TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text19", "TEXT"));
            TimberLoggerAdvanced.logParams(taskId, LogParams.create().text("text20", "TEXT"));
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

    @TimberLogTask(name = "simple_stress")
    private String simpleStressLog() {
        String currentTaskId = TimberLogger.getCurrentTaskId();
        TimberLogger.logText("text1", "Text");
        TimberLogger.logText("text2", "Text");
        TimberLogger.logText("text3", "Text");
        TimberLogger.logText("text4", "Text");
        TimberLogger.logText("text5", "Text");
        TimberLogger.logText("text6", "Text");
        TimberLogger.logText("text7", "Text");
        TimberLogger.logText("text8", "Text");
        TimberLogger.logText("text9", "Text");
        TimberLogger.logText("text10", "Text");
        TimberLogger.logText("text11", "Text");
        TimberLogger.logText("text12", "Text");
        TimberLogger.logText("text13", "Text");
        TimberLogger.logText("text14", "Text");
        TimberLogger.logText("text15", "Text");
        TimberLogger.logText("text16", "Text");
        TimberLogger.logText("text17", "Text");
        TimberLogger.logText("text18", "Text");
        TimberLogger.logText("text19", "Text");
        TimberLogger.logText("text20", "Text");
        return currentTaskId;
    }
}

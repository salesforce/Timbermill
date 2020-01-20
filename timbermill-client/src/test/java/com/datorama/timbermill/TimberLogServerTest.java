package com.datorama.timbermill;

import com.datorama.timbermill.pipe.TimbermillServerOutputPipe;
import org.junit.BeforeClass;
import org.junit.Test;

public class TimberLogServerTest extends TimberLogTest{

    private static String timbermillUrl = "http://localhost:8484";

    @BeforeClass
    public static void init() {
        try {
            timbermillUrl = System.getenv("TIMBERMILL_URL");
        } catch (Throwable ignored){}
        TimbermillServerOutputPipe pipe = new TimbermillServerOutputPipe.Builder().timbermillServerUrl(timbermillUrl + "/events").build();
        TimberLogger.bootstrap(pipe, TEST);
    }

    @Test
    public void testSimpleTaskIndexerJob() throws InterruptedException {
       super.testSimpleTaskIndexerJob();
    }

    @Test
    public void testSwitchCasePlugin() {
        super.testSwitchCasePlugin();
    }

    @Test
    public void testSimpleTaskWithTrimmer() {
        super.testSimpleTaskWithTrimmer();
    }

    @Test
    public void testSpotWithParent(){
        super.testSpotWithParent();
    }

    @Test
    public void testSimpleTasksFromDifferentThreadsIndexerJob(){
        super.testSimpleTasksFromDifferentThreadsIndexerJob();
    }

    @Test
    public void testSimpleTasksFromDifferentThreadsWithWrongParentIdIndexerJob() {
        super.testSimpleTasksFromDifferentThreadsWithWrongParentIdIndexerJob();
    }

    @Test
    public void testComplexTaskIndexerWithErrorTask() {
        super.testComplexTaskIndexerWithErrorTask();
    }

    @Test
    public void testTaskWithNullString() {
        super.testTaskWithNullString();
    }

    @Test
    public void testOverConstructor() {
        super.testOverConstructor();
    }

    @Test
    public void testOverConstructorException() {
       super.testOverConstructorException();
    }

    @Test
    public void testCorruptedInfoOnly() {
        super.testCorruptedInfoOnly();
    }

    @Test
    public void testOrphan() {
        super.testOrphan();
    }
}

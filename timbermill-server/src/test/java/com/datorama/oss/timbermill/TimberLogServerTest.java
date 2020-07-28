package com.datorama.oss.timbermill;

import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datorama.oss.timbermill.pipe.TimbermillServerOutputPipe;
import com.datorama.oss.timbermill.pipe.TimbermillServerOutputPipeBuilder;

import static com.datorama.oss.timbermill.common.Constants.DEFAULT_TIMBERMILL_URL;

public class TimberLogServerTest extends TimberLogTest{

    private static TimbermillServerOutputPipe pipe;

    @BeforeClass
    public static void init()  {
        String timbermillUrl = System.getenv("TIMBERMILL_URL");
        if (StringUtils.isEmpty(timbermillUrl)){
            timbermillUrl = DEFAULT_TIMBERMILL_URL;
        }
        pipe = new TimbermillServerOutputPipeBuilder().timbermillServerUrl(timbermillUrl).maxBufferSize(200000000)
                .maxSecondsBeforeBatchTimeout(3).build();
        TimberLogTest.init(pipe);
    }

    @AfterClass
    public static void tearDown(){
        TimberLogTest.tearDown();
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

    @Test
    public void testMissingParentTaskFromDifferentThreads(){
        super.testMissingParentTaskFromDifferentThreads();
    }

    @Test
    public void testMissingParentTaskOutOffOrderFromDifferentThreads(){
        super.testMissingParentTaskOutOffOrderFromDifferentThreads();
    }


}

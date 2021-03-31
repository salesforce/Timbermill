package com.datorama.oss.timbermill;

import com.datorama.oss.timbermill.common.ElasticsearchUtil;
import com.datorama.oss.timbermill.pipe.TimbermillServerOutputPipe;
import com.datorama.oss.timbermill.pipe.TimbermillServerOutputPipeBuilder;
import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TimberLogServerTest extends TimberLogTest{

	static final String DEFAULT_TIMBERMILL_URL = "http://localhost:8484";

    @BeforeClass
    public static void init()  {
        String timbermillUrl = System.getenv("TIMBERMILL_URL");
        if (StringUtils.isEmpty(timbermillUrl)){
            timbermillUrl = DEFAULT_TIMBERMILL_URL;
        }
        TimbermillServerOutputPipe pipe = new TimbermillServerOutputPipeBuilder().timbermillServerUrl(timbermillUrl).maxBufferSize(200000000)
                .maxSecondsBeforeBatchTimeout(3).numOfThreads(1).build();
        ElasticsearchUtil.getEnvSet().add(TEST);
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
    public void testSimpleTaskWithParams(){
        super.testSimpleTaskWithParams();
    }

    @Test
    public void testEndWithingAnnotation(){
        super.testEndWithingAnnotation();
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
        super.testMissingParentTaskFromDifferentThreads(false);
    }

    @Test
    public void testMissingParentTaskFromDifferentThreadsRollover(){
        super.testMissingParentTaskFromDifferentThreads(true);
    }

    @Test
    public void testMissingParentTaskOutOffOrderFromDifferentThreads(){
        super.testMissingParentTaskOutOffOrderFromDifferentThreads(false);
    }

    @Test
    public void testMissingParentTaskOutOffOrderFromDifferentThreadsRollover(){
        super.testMissingParentTaskOutOffOrderFromDifferentThreads(true);
    }

}

package com.datorama.timbermill;

import com.datorama.timbermill.common.Constants;
import com.datorama.timbermill.pipe.TimbermillServerOutputPipe;
import com.datorama.timbermill.pipe.TimbermillServerOutputPipeBuilder;

import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class TimberLogServerTest extends TimberLogTest{

    @BeforeClass
    public static void init() {
        String timbermillUrl = System.getenv("TIMBERMILL_URL");
        if (StringUtils.isEmpty(timbermillUrl)){
            timbermillUrl = Constants.DEFAULT_TIMBERMILL_URL;
        }
        String elasticUrl = System.getenv("ELASTICSEARCH_URL");
        if (StringUtils.isEmpty(elasticUrl)){
            elasticUrl = Constants.DEFAULT_ELASTICSEARCH_URL;
        }
        client = new ElasticsearchClient(elasticUrl, 1000, 1, null, null, null,
                7, 100, 1000000000);
        TimbermillServerOutputPipe pipe = new TimbermillServerOutputPipeBuilder().timbermillServerUrl(timbermillUrl).build();
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

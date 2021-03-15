package com.datorama.oss.timbermill;

import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datorama.oss.timbermill.pipe.LocalOutputPipe;

public class TimberLogLocalTest extends TimberLogTest {

    @BeforeClass
    public static void init(){
        String elasticUrl = System.getenv("ELASTICSEARCH_URL");
        if (StringUtils.isEmpty(elasticUrl)){
            elasticUrl = "http://localhost:9200";
        }
        LocalOutputPipe pipe = new LocalOutputPipe.Builder().numberOfShards(1).numberOfReplicas(0).url(elasticUrl).deletionCronExp(null).persistenceHandlerStrategy(null).deletionCronExp("")
                .bulkPersistentFetchCronExp("").eventsPersistentFetchCronExp("").mergingCronExp("")
                .pluginsJson("[{\"class\":\"SwitchCasePlugin\",\"taskMatcher\":{\"name\":\"" + EVENT + "plugin"
                        + "\"},\"searchField\":\"exception\",\"outputAttribute\":\"errorType\",\"switchCase\":[{\"match\":[\"TOO_MANY_SERVER_ROWS\"],\"output\":\"TOO_MANY_SERVER_ROWS\"},{\"match\":[\"PARAMETER_MISSING\"],\"output\":\"PARAMETER_MISSING\"},{\"match\":[\"Connections could not be acquired\",\"terminating connection due to administrator\",\"connect timed out\"],\"output\":\"DB_CONNECT\"},{\"match\":[\"did not fit in memory\",\"Insufficient resources to execute plan\",\"Query exceeded local memory limit\",\"ERROR: Plan memory limit exhausted\"],\"output\":\"DB_RESOURCES\"},{\"match\":[\"Invalid input syntax\",\"SQLSyntaxErrorException\",\"com.facebook.presto.sql.parser.ParsingException\",\"com.facebook.presto.sql.analyzer.SemanticException\",\"org.postgresql.util.PSQLException: ERROR: missing FROM-clause entry\",\"org.postgresql.util.PSQLException: ERROR: invalid input syntax\"],\"output\":\"DB_SQL_SYNTAX\"},{\"match\":[\"Execution canceled by operator\",\"InterruptedException\",\"Execution time exceeded run time cap\",\"TIME_OUT\",\"canceling statement due to user request\",\"Caused by: java.net.SocketTimeoutException: Read timed out\"],\"output\":\"DB_QUERY_TIME_OUT\"},{\"output\":\"DB_UNKNOWN\"}]}]")
                .build();
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

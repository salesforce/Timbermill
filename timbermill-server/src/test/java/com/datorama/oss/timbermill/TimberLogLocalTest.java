package com.datorama.oss.timbermill;

import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datorama.oss.timbermill.pipe.LocalOutputPipe;

import static com.datorama.oss.timbermill.common.Constants.DEFAULT_ELASTICSEARCH_URL;

public class TimberLogLocalTest extends TimberLogTest {

    private static LocalOutputPipe pipe;

    @BeforeClass
    public static void init(){
        String elasticUrl = System.getenv("ELASTICSEARCH_URL");
        if (StringUtils.isEmpty(elasticUrl)){
            elasticUrl = DEFAULT_ELASTICSEARCH_URL;
        }
		pipe = new LocalOutputPipe.Builder().numberOfShards(1).numberOfReplicas(0).url(elasticUrl).deletionCronExp(null).diskHandlerStrategy(null)
				.pluginsJson("[{\"class\":\"SwitchCasePlugin\",\"taskMatcher\":{\"name\":\""+ EVENT + "plugin" + "\"},\"searchField\":\"exception\",\"outputAttribute\":\"errorType\",\"switchCase\":[{\"match\":[\"TOO_MANY_SERVER_ROWS\"],\"output\":\"TOO_MANY_SERVER_ROWS\"},{\"match\":[\"PARAMETER_MISSING\"],\"output\":\"PARAMETER_MISSING\"},{\"match\":[\"Connections could not be acquired\",\"terminating connection due to administrator\",\"connect timed out\"],\"output\":\"DB_CONNECT\"},{\"match\":[\"did not fit in memory\",\"Insufficient resources to execute plan\",\"Query exceeded local memory limit\",\"ERROR: Plan memory limit exhausted\"],\"output\":\"DB_RESOURCES\"},{\"match\":[\"Invalid input syntax\",\"SQLSyntaxErrorException\",\"com.facebook.presto.sql.parser.ParsingException\",\"com.facebook.presto.sql.analyzer.SemanticException\",\"org.postgresql.util.PSQLException: ERROR: missing FROM-clause entry\",\"org.postgresql.util.PSQLException: ERROR: invalid input syntax\"],\"output\":\"DB_SQL_SYNTAX\"},{\"match\":[\"Execution canceled by operator\",\"InterruptedException\",\"Execution time exceeded run time cap\",\"TIME_OUT\",\"canceling statement due to user request\",\"Caused by: java.net.SocketTimeoutException: Read timed out\"],\"output\":\"DB_QUERY_TIME_OUT\"},{\"output\":\"DB_UNKNOWN\"}]}]")
				.build();
        client = new ElasticsearchClient(elasticUrl, 1000, 1, null, null, null,
                7, 100, 1000000000,3, 3, 1000,null ,1, 1, 4000);
        TimberLogger.bootstrap(pipe, TEST);
    }

    @AfterClass
    public static void tearDown(){
        pipe.close();
        client.close();
    }

//    @Test
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

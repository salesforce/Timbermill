package com.datorama.oss.timbermill;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.datorama.oss.timbermill.common.*;
import com.datorama.oss.timbermill.pipe.LocalOutputPipe;

import static com.datorama.oss.timbermill.common.Constants.DEFAULT_ELASTICSEARCH_URL;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TimberLogLocalTest extends TimberLogTest {

    @BeforeClass
    public static void init()  {
        String elasticUrl = System.getenv("ELASTICSEARCH_URL");
        if (StringUtils.isEmpty(elasticUrl)){
            elasticUrl = DEFAULT_ELASTICSEARCH_URL;
        }
        LocalOutputPipe.Builder pipeBuilder = new LocalOutputPipe.Builder().numberOfShards(1).numberOfReplicas(0).url(elasticUrl).deletionCronExp(null)
                .pluginsJson("[{\"class\":\"SwitchCasePlugin\",\"taskMatcher\":{\"name\":\""+ EVENT + "plugin" + "\"},\"searchField\":\"exception\",\"outputAttribute\":\"errorType\",\"switchCase\":[{\"match\":[\"TOO_MANY_SERVER_ROWS\"],\"output\":\"TOO_MANY_SERVER_ROWS\"},{\"match\":[\"PARAMETER_MISSING\"],\"output\":\"PARAMETER_MISSING\"},{\"match\":[\"Connections could not be acquired\",\"terminating connection due to administrator\",\"connect timed out\"],\"output\":\"DB_CONNECT\"},{\"match\":[\"did not fit in memory\",\"Insufficient resources to execute plan\",\"Query exceeded local memory limit\",\"ERROR: Plan memory limit exhausted\"],\"output\":\"DB_RESOURCES\"},{\"match\":[\"Invalid input syntax\",\"SQLSyntaxErrorException\",\"com.facebook.presto.sql.parser.ParsingException\",\"com.facebook.presto.sql.analyzer.SemanticException\",\"org.postgresql.util.PSQLException: ERROR: missing FROM-clause entry\",\"org.postgresql.util.PSQLException: ERROR: invalid input syntax\"],\"output\":\"DB_SQL_SYNTAX\"},{\"match\":[\"Execution canceled by operator\",\"InterruptedException\",\"Execution time exceeded run time cap\",\"TIME_OUT\",\"canceling statement due to user request\",\"Caused by: java.net.SocketTimeoutException: Read timed out\"],\"output\":\"DB_QUERY_TIME_OUT\"},{\"output\":\"DB_UNKNOWN\"}]}]");
        LocalOutputPipe pipe = buildLocalOutputPipeForTest(pipeBuilder,diskHandler);

        client = new ElasticsearchClient(elasticUrl, 1000, 1, null, null, null,
                7, 100, 1000000000,3, 3,3,null);
        TimberLogger.bootstrap(pipe, TEST);
    }

    @AfterClass
    public static void tearDown(){
        client.close();
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


    private static LocalOutputPipe buildLocalOutputPipeForTest(LocalOutputPipe.Builder pipeBuilder,SQLJetDiskHandler diskHandler) {
        ElasticsearchParams elasticsearchParams = pipeBuilder.buildElasticSearchParams();
        ElasticsearchClient elasticsearchClient = pipeBuilder.buildElasticSearchClient(diskHandler);
        ElasticsearchClient elasticsearchClientSpy = Mockito.spy(elasticsearchClient);
        if (testWithPersistence){
            try {
                doAnswer(new Answer<BulkResponse>() {
                    @Override public BulkResponse answer(InvocationOnMock invocation) throws Throwable {
                        Object[] args = invocation.getArguments();
                        DbBulkRequest dbBulkRequest = (DbBulkRequest) args[0];
                        if (dbBulkRequest.getTimesFetched() < 2){
                            throw new RuntimeException();
                        }
                        // call real method
                        return elasticsearchClient.bulk(dbBulkRequest,(RequestOptions)args[1]);
                    }
                }).when(elasticsearchClientSpy).bulk(any(),any());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return new LocalOutputPipe(elasticsearchParams, elasticsearchClientSpy, diskHandler);
    }

}

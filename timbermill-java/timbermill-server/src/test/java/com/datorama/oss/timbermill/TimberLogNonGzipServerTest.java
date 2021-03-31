package com.datorama.oss.timbermill;

import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.datorama.oss.timbermill.common.ElasticsearchUtil;
import com.datorama.oss.timbermill.pipe.TimbermillServerOutputPipe;
import com.datorama.oss.timbermill.pipe.TimbermillServerOutputPipeBuilder;

public class TimberLogNonGzipServerTest extends TimberLogTest{

	static final String DEFAULT_TIMBERMILL_URL = "http://localhost:8484";

    @BeforeClass
    public static void init()  {
        String timbermillUrl = System.getenv("TIMBERMILL_URL");
        if (StringUtils.isEmpty(timbermillUrl)){
            timbermillUrl = DEFAULT_TIMBERMILL_URL;
        }
        TimbermillServerOutputPipe pipe = new TimbermillServerOutputPipeBuilder().timbermillServerUrl(timbermillUrl).maxBufferSize(200000000)
                .maxSecondsBeforeBatchTimeout(3).numOfThreads(1).sendGzippedRequest(false).build();
        ElasticsearchUtil.getEnvSet().add(TEST);
        TimberLogTest.init(pipe);
    }

    @AfterClass
    public static void tearDown(){
        TimberLogTest.tearDown();
    }

    @Test
    public void testSimpleTaskWithParams(){
        super.testSimpleTaskWithParams();
    }

}

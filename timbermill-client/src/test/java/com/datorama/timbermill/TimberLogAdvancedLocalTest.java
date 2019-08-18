package com.datorama.timbermill;

import org.junit.BeforeClass;
import org.junit.Test;

public class TimberLogAdvancedLocalTest extends TimberLogAdvancedTest {

    @BeforeClass
    public static void init() {
        TimberLogLocalTest.init();
    }

    @Test
    public void testOngoingTask() {
        super.testOngoingTask();
    }

    @Test
    public void testOutOfOrderTask() {
        super.testOutOfOrderTask();
    }

    @Test
    public void testOutOfOrderWithParentTask() {
        super.testOutOfOrderWithParentTask();
    }

    @Test
    public void testOutOfOrderTaskError() {
        super.testOutOfOrderTaskError();
    }

    @Test
    public void testOutOfOrderTaskStartSuccessLog() {
        super.testOutOfOrderTaskStartSuccessLog();
    }

    @Test
    public void testOutOfOrderTaskLogStartSuccess() {
       super.testOutOfOrderTaskLogStartSuccess();
    }

    @Test
    public void testOutOfOrderTaskSuccessLogStart() {
        super.testOutOfOrderTaskSuccessLogStart();
    }

    @Test
    public void testOutOfOrderTaskSuccessStartLog() {
       super.testOutOfOrderTaskSuccessStartLog();
    }

    @Test
    public void testOutOfOrderTaskSuccessLogNoStart() {
        super.testOutOfOrderTaskSuccessLogNoStart();
    }

    @Test
    public void testOutOfOrderTaskErrorLogNoStart() {
        super.testOutOfOrderTaskErrorLogNoStart();
    }

    @Test
    public void testOutOfOrderTaskLogSuccessNoStart() {
        super.testOutOfOrderTaskLogSuccessNoStart();
    }

    @Test
    public void testOutOfOrderTaskStartLogNoSuccess() {
        super.testOutOfOrderTaskStartLogNoSuccess();
    }

    @Test
    public void testOutOfOrderTaskLogStartNoSuccess() {
        super.testOutOfOrderTaskLogStartNoSuccess();
    }

    @Test
    public void testOnlyLog() {
        super.testOnlyLog();
    }

    @Test
    public void testOngoingPrimaryTask() {
        super.testOngoingPrimaryTask();
    }

    @Test
    public void testOngoingTaskWithContext() {
        super.testOngoingTaskWithContext();
    }

    @Test
    public void testOngoingTaskWithNullContext() {
        super.testOngoingTaskWithNullContext();
    }
}

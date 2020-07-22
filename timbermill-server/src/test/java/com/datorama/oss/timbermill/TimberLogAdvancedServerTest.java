package com.datorama.oss.timbermill;


import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TimberLogAdvancedServerTest extends TimberLogAdvancedTest {

    @BeforeClass
    public static void init() {
        TimberLogServerTest.init();
    }

    @AfterClass
    public static void tearDown(){
        TimberLogServerTest.tearDown();
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

    @Test
    public void testIncorrectTaskStartSuccessStart() throws InterruptedException {
        super.testIncorrectTaskStartSuccessStart(false);
    }

    @Test
    public void testIncorrectTaskStartSuccessStartWithUpdate() throws InterruptedException {
        super.testIncorrectTaskStartSuccessStart(true);
    }

    @Test
    public void testIncorrectTaskStartSuccessStartSuccess() throws InterruptedException {
        super.testIncorrectTaskStartSuccessStartSuccess(false);
    }

    @Test
    public void testIncorrectTaskStartSuccessStartSuccessWithUpdate() throws InterruptedException {
        super.testIncorrectTaskStartSuccessStartSuccess(true);
    }

    @Test
    public void testIncorrectTaskStartSuccessSuccess() throws InterruptedException {
        super.testIncorrectTaskStartSuccessSuccess(false);
    }

    @Test
    public void testIncorrectTaskStartSuccessSuccessWithUpdate() throws InterruptedException {
        super.testIncorrectTaskStartSuccessSuccess(true);
    }

    @Test
    public void testIncorrectTaskStartSuccessError() throws InterruptedException {
        super.testIncorrectTaskStartSuccessError(false);
    }

    @Test
    public void testIncorrectTaskStartSuccessErrorWithUpdate() throws InterruptedException {
        super.testIncorrectTaskStartSuccessError(true);
    }

    @Test
    public void testIncorrectTaskStartStartSuccess() throws InterruptedException {
        super.testIncorrectTaskStartStartSuccess(false);
    }

    @Test
    public void testIncorrectTaskStartStartSuccessWithUpdate() throws InterruptedException {
        super.testIncorrectTaskStartStartSuccess(true);
    }

    @Test
    public void testIncorrectTaskStartStart() throws InterruptedException {
        super.testIncorrectTaskStartStart(false);
    }

    @Test
    public void testIncorrectTaskStartStartWithUpdate() throws InterruptedException {
        super.testIncorrectTaskStartStart(true);
    }

    @Test
    public void testIncorrectTaskSuccessStartSuccess() throws InterruptedException {
        super.testIncorrectTaskSuccessStartSuccess(false);
    }

    @Test
    public void testIncorrectTaskSuccessStartSuccessWithUpdate() throws InterruptedException {
        super.testIncorrectTaskSuccessStartSuccess(true);
    }

    @Test
    public void testIncorrectTaskSuccessSuccess() throws InterruptedException {
        super.testIncorrectTaskSuccessSuccess(false);
    }

    @Test
    public void testIncorrectTaskSuccessSuccessWithUpdate() throws InterruptedException {
        super.testIncorrectTaskSuccessSuccess(true);
    }

    @Test
    public void testIncorrectTaskSuccessError() throws InterruptedException {
        super.testIncorrectTaskSuccessError(false);
    }

    @Test
    public void testIncorrectTaskSuccessErrorWithUpdate() throws InterruptedException {
        super.testIncorrectTaskSuccessError(true);
    }

    @Test
    public void testIncorrectTaskErrorStartSuccess() throws InterruptedException {
        super.testIncorrectTaskErrorStartSuccess(false);
    }

    @Test
    public void testIncorrectTaskErrorStartSuccessWithUpdate() throws InterruptedException {
        super.testIncorrectTaskErrorStartSuccess(true);
    }

    @Test
    public void testIncorrectTaskErrorSuccess() throws InterruptedException {
        super.testIncorrectTaskErrorSuccess(false);
    }

    @Test
    public void testIncorrectTaskErrorSuccessWithUpdate() throws InterruptedException {
        super.testIncorrectTaskErrorSuccess(true);
    }

    @Test
    public void testIncorrectTaskErrorError() throws InterruptedException {
        super.testIncorrectTaskErrorError(false);
    }

    @Test
    public void testIncorrectTaskErrorErrorWithUpdate() throws InterruptedException {
        super.testIncorrectTaskErrorError(true);
    }

    @Test
    public void testIncorrectTaskStartErrorStart() throws InterruptedException {
        super.testIncorrectTaskStartErrorStart(false);
    }

    @Test
    public void testIncorrectTaskStartErrorStartWithUpdate() throws InterruptedException {
        super.testIncorrectTaskStartErrorStart(true);
    }
}

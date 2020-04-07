package com.datorama.oss.timbermill;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TimberLogAdvancedOrphansLocalTest extends TimberLogAdvancedOrphansTest {

    @BeforeClass
    public static void init() {
        TimberLogLocalTest.init();
    }

    @AfterClass
    public static void tearDown(){
        TimberLogLocalTest.tearDown();
    }

    @Test
    public void testOrphanIncorrectOrder() {
        super.testOrphanIncorrectOrder();
    }

    @Test
    public void testOrphanWithAdoption(){
        super.testOrphanWithAdoption();
    }

    @Test
    public void testOrphanWithComplexAdoption(){
        super.testOrphanWithComplexAdoption();
    }

    @Test
    public void testOutOfOrderComplexOrphanWithAdoption(){
        super.testOutOfOrderComplexOrphanWithAdoption();
    }

    @Test
    public void testInOrderComplexOrphanWithAdoption(){
        super.testInOrderComplexOrphanWithAdoption();
    }

    @Test
    public void testOrphanWithAdoptionDifferentBatches(){
        super.testOrphanWithAdoptionDifferentBatches();
    }

    @Test
    public void testStringOfOrphans(){
        super.testStringOfOrphans();
    }
}

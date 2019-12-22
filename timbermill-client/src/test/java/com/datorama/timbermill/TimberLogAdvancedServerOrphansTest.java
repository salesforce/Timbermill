package com.datorama.timbermill;

import org.junit.BeforeClass;
import org.junit.Test;

public class TimberLogAdvancedServerOrphansTest extends TimberLogAdvancedOrphansTest{

	@BeforeClass
	public static void init() {
		TimberLogServerTest.init();
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
	public void testOutOfOrderComplexOrphanWithAdoption(){
		super.testOutOfOrderComplexOrphanWithAdoption();
	}

    @Test
    public void testInOrderComplexOrphanWithAdoption(){
        super.testInOrderComplexOrphanWithAdoption();
    }
}

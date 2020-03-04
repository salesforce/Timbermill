package com.datorama.oss.timbermill;

import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Test;

public class TimberLogAdvancedOrphansServerTest extends TimberLogAdvancedOrphansTest{

	@BeforeClass
	public static void init() throws SQLException {
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

	@Test
	public void testOrphanWithAdoptionDifferentBatches(){
		super.testOrphanWithAdoptionDifferentBatches();
	}

	@Test
	public void testStringOfOrphans(){
		super.testStringOfOrphans();
	}
}

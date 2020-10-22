package com.datorama.oss.timbermill;


import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TimberLogAdvancedOrphansServerTest extends TimberLogAdvancedOrphansTest{

	@BeforeClass
	public static void init() {
		TimberLogServerTest.init();
	}

	@AfterClass
	public static void tearDown(){
		TimberLogServerTest.tearDown();
	}

	@Test
	public void testOrphanIncorrectOrder() {
		super.testOrphanIncorrectOrder();
	}

	@Test
	public void testOrphanWithAdoption(){
		super.testOrphanWithAdoption(false);
	}

	@Test
	public void testOrphanWithAdoptionRollover(){
		super.testOrphanWithAdoption(true);
	}

	@Test
	public void testOrphanWithAdoptionParentWithNoStart(){
		super.testOrphanWithAdoptionParentWithNoStart(false);
	}

	@Test
	public void testOrphanWithAdoptionParentWithNoStartRollover(){
		super.testOrphanWithAdoptionParentWithNoStart(true);
	}

	@Test
	public void testOrphanWithComplexAdoption(){
		super.testOrphanWithComplexAdoption(false);
	}

	@Test
	public void testOrphanWithComplexAdoptionRollover(){
		super.testOrphanWithComplexAdoption(true);
	}

	@Test
	public void testOutOfOrderComplexOrphanWithAdoption(){
		super.testOutOfOrderComplexOrphanWithAdoption(false);
	}

	@Test
	public void testOutOfOrderComplexOrphanWithAdoptionRollover(){
		super.testOutOfOrderComplexOrphanWithAdoption(true);
	}

	@Test
	public void testInOrderComplexOrphanWithAdoption(){
		super.testInOrderComplexOrphanWithAdoption(false);
	}

	@Test
	public void testInOrderComplexOrphanWithAdoptionRollover(){
		super.testInOrderComplexOrphanWithAdoption(true);
	}

	@Test
	public void testStringOfOrphans(){
		super.testStringOfOrphans();
	}

	@Test
	public void testOrphanWithAdoptionFromDifferentNode() {super.testOrphanWithAdoptionFromDifferentNode(false); }

	@Test
	public void testOrphanWithAdoptionFromDifferentNodeRollover() {super.testOrphanWithAdoptionFromDifferentNode(true); }


	@Test
	public void testOrphanWithChainAdoptionFromDifferentNode() {super.testOrphanWithChainAdoptionFromDifferentNode(false); }

	@Test
	public void testOrphanWithChainAdoptionFromDifferentNodeRollover() {super.testOrphanWithChainAdoptionFromDifferentNode(true); }
}

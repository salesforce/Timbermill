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
		super.testOrphanWithAdoption();
	}

	@Test
	public void testOrphanWithAdoptionParentWithNoStart(){
		super.testOrphanWithAdoptionParentWithNoStart();
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
	public void testStringOfOrphans(){
		super.testStringOfOrphans();
	}

	@Test
	public void testOrphanWithAdoptionFromDifferentNode() {super.testOrphanWithAdoptionFromDifferentNode(); }

	@Test
	public void testOrphanWithChainAdoptionFromDifferentNode() {super.testOrphanWithChainAdoptionFromDifferentNode(); }
}

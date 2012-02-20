package com.datasalt.pangool.flow;

import static org.junit.Assert.*;

import org.junit.Test;

import com.datasalt.pangool.flow.LinearFlow.EXECUTION_MODE;

public class TestFlow {

	@Test
	public void test() throws Exception {
		ExampleFlow flow = new ExampleFlow();
		flow.execute("job3.output", EXECUTION_MODE.POLITE);
		assertEquals(3, flow.executedJobs.size());
		
		assertEquals("job2", flow.executedJobs.get(0));
		assertEquals("job1", flow.executedJobs.get(1));
		assertEquals("job3", flow.executedJobs.get(2));
	}
}

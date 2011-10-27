package com.datasalt.pangolin.commons.flow;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestFlow {

	@Test
	public void test() throws Exception {
		
		final StringBuffer sbOrder = new StringBuffer();
		final StringBuffer sbWrite = new StringBuffer();
		
		Executable<String> exA = new Executable<String>() {

			@Override
      public void execute(String configData) throws Exception {
				sbWrite.append(configData);
				sbOrder.append("A");
      }
			
		};
		
		Executable<String> exB = new Executable<String>() {

			@Override
      public void execute(String configData) throws Exception {
				sbWrite.append(configData);
				sbOrder.append("B");
      }
			
		};
		
		Executable<String> exC = new Executable<String>() {

			@Override
      public void execute(String configData) throws Exception {
				sbWrite.append(configData);
				sbOrder.append("C");
      }
			
		};

		// Testing split
		Flow<String> flowRoot = Flow.newFlow();
		Flow<String> sf1 = new Flow<String>().chain("c1", "c1des", exA).chain("c2", "c2des", exB);
		Flow<String> sf2 = new Flow<String>().chain("c3", "c3des", exC);
		flowRoot.split(sf1,
				sf2);
		
		flowRoot.execute("A");
		flowRoot.executeFrom(Flow.ROOT_FLOW_NAME, "B");
		assertEquals("ABCABC", sbOrder.toString());
		assertEquals("AAABBB", sbWrite.toString());
		assertTrue(flowRoot.getNames().contains("c1"));
		assertTrue(flowRoot.getNames().contains("c2"));
		assertTrue(flowRoot.getNames().contains("c3"));
		
		sbWrite.setLength(0);
		sbOrder.setLength(0);
		
		flowRoot = Flow.newFlow();
		flowRoot.barrier("Barrier", "b", sf1, sf2).chain("c4", "c4des", exA);

		flowRoot.execute("A");
		flowRoot.executeFrom("Barrier", "B");
		assertEquals("ABCAABCA", sbOrder.toString());
		assertEquals("AAAABBBB", sbWrite.toString());
		
		assertTrue(flowRoot.getNames().contains("c4"));
		assertTrue(flowRoot.getNames().contains("c1"));
		assertEquals("b", flowRoot.getDescription("Barrier"));
		
		
		sbWrite.setLength(0);
		sbOrder.setLength(0);
		
		flowRoot = Flow.newFlow();
		flowRoot.chain(sf1);
		flowRoot.chain(sf2);
		
		flowRoot.execute("A");
		flowRoot.executeFrom("c2", "B");
		assertEquals("ABCBC", sbOrder.toString());
		
		
		System.out.println(flowRoot.getHelp());
		

	}
}

package com.datasalt.pangolin.commons.flow;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestChainableExecutable {

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


 		
		ChainableExecutable<String> c1 = new ChainableExecutable<String>("c1", "c1des", null, exA);
		ChainableExecutable<String> c2 = new ChainableExecutable<String>("c2", "c2des", c1, exB);
		ChainableExecutable<String> c3 = new ChainableExecutable<String>("c3", "c3des", c1, exC);
		c3.registerChild(exC);
		
		c1.execute("A");
		
		assertEquals("ABCC", sbOrder.toString());
		assertEquals("AAAA", sbWrite.toString());
		assertEquals(c1.getName(), "c1");
		assertEquals(c1.getDescription(), "c1des");
		
	}
}

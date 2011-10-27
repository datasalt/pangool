package com.datasalt.pangolin.commons.flow;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestBarrierExecutable {

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
		
	 		
		BarrierExecutable<String> c1 = new BarrierExecutable<String>("c1", "c1des", null);
		ChainableExecutable<String> c3 = new ChainableExecutable<String>("c3", "c3des", c1, exB);
		ChainableExecutable<String> c2 = new ChainableExecutable<String>("c2", "c2des", null, exA);
		c1.addBarriedChild(c2);
		
		c1.execute("A");
		
		assertEquals("AB", sbOrder.toString());
		assertEquals("AA", sbWrite.toString());
		
	}
}

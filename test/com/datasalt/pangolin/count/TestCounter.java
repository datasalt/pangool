package com.datasalt.pangolin.count;

import static org.junit.Assert.*;





import org.junit.Test;

import com.datasalt.pangolin.commons.count.Counter;
import com.datasalt.pangolin.commons.count.Counter.Count;
import com.datasalt.pangolin.commons.count.Counter.CounterException;

public class TestCounter {
	
	@Test
	public void testDistinct() throws CounterException {

		Counter c = Counter.createWithDistinctElements();
		c.in("Animals").in("Dog").in("Mastin").count("toby");
		c.in("Animals").in("Dog").in("Mastin").count("toby");
		c.in("Animals").in("Dog").in("Mastin").count("toby");
		c.in("Animals").in("Dog").in("Mastin").count("toby");
		
		Count co = c.getCounts();
		assertEquals(1, co.getDistinctList().size());
		assertEquals(1, co.get("Animals").getDistinctList().size());
		assertEquals(1, co.get("Animals").get("Dog").getDistinctList().size());
		assertEquals(1, co.get("Animals").get("Dog").get("Mastin").getDistinctList().size());
	}
	
	@Test
	public void test() throws Counter.CounterException {
		Counter c = Counter.createWithDistinctElements();
		c.in("Animals").in("Dog").in("Mastin").count("toby");
		c.in("Animals").in("Dog").in("BullDog").count("toby");
		c.in("Animals").in("Dog").in("BullDog").count("toby");
		c.in("Animals").in("Dog").in("BullDog").count("brian");
		
		Count co = c.getCounts();
		assertEquals(4, co.getCount());
		assertEquals(2, co.getDistinctList().size());
		
		assertEquals(4, co.get("Animals").get("Dog").getCount());
		assertEquals(2, co.get("Animals").get("Dog").getDistinctList().size());

		assertEquals(1, co.get("Animals").get("Dog").get("Mastin").getCount());
		assertEquals(1, co.get("Animals").get("Dog").get("Mastin").getDistinctList().size());
		
		assertEquals(3, co.get("Animals").get("Dog").get("BullDog").getCount());
		assertEquals(2, co.get("Animals").get("Dog").get("BullDog").getDistinctList().size());
	}
	
}

package com.datasalt.pangool.mapreduce;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.junit.Assert;
import org.junit.Test;

import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.io.tuple.ITuple.InvalidFieldException;

public class TestSortComparator extends ComparatorsBaseTest {

	@Test
	public void testObjectComparison() throws CoGrouperException, JsonGenerationException, JsonMappingException, IOException, InvalidFieldException  {
		SortComparator c = new SortComparator();
		setConf(c);
		
		// source 1
		Assert.assertEquals(0, c.compare(getTuple1(true, 10, "a"), getTuple1(true, 10, "a")));
		assertNegative(c,getTuple1(false, 10, "a"), getTuple1(true, 10, "a"));
		assertPositive(c,getTuple1(true, 10, "a"), getTuple1(false, 10, "a"));
		assertPositive(c,getTuple1(true, 1, "a"), getTuple1(true, 10, "a"));
		assertNegative(c,getTuple1(true, 10, "a"), getTuple1(true, 1, "a"));
		assertNegative(c,getTuple1(true, 10, "b"), getTuple1(true, 10, "a"));
		assertPositive(c,getTuple1(true, 10, "a"), getTuple1(true, 10, "b"));
		
//		// Different sources comparing
		assertPositive(c,getTuple1(true, 10, ""), getTuple2(true, 10, -1));
		assertNegative(c,getTuple2(true, 10, -1), getTuple1(true, 10, ""));
//
		// source 2
		Assert.assertEquals(0, c.compare(getTuple2(true, 10, 0), getTuple2(true, 10, 0)));
		assertNegative(c,getTuple2(false, 10, 0), getTuple2(true, 10, 0));
		assertPositive(c,getTuple2(true, 10, 0), getTuple2(false, 10, 0));
		assertPositive(c,getTuple2(true, 1, 0), getTuple2(true, 10, 0));
		assertNegative(c,getTuple2(true, 10, 0), getTuple2(true, 1, 0));
		assertPositive(c,getTuple2(true, 10, 0), getTuple2(true, 10, 10));
		assertNegative(c,getTuple2(true, 10, 10), getTuple2(true, 10, 0));
	}
	
	
}

package com.datasalt.pangool.io.tuple;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.junit.Assert;
import org.junit.Test;

import com.datasalt.pangolin.grouper.io.tuple.BaseTuple;
import com.datasalt.pangolin.grouper.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.PangoolConfig;
import com.datasalt.pangool.PangoolConfigBuilder;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.SortCriteria.SortOrder;
import com.datasalt.pangool.SortingBuilder;

public class TestSortComparator {

	public SourcedTuple getTuple1(boolean booleanValue, int intValue, String strValue) {
		SourcedTuple tuple = new SourcedTuple(new BaseTuple());
		tuple.setBoolean("booleanField", booleanValue);
		tuple.setInt("intField", intValue);
		tuple.setString("strField", strValue);
		tuple.setSource(1);
		return tuple;
	}
	
	public SourcedTuple getTuple2(boolean booleanValue, int intValue, long longValue) {
		SourcedTuple tuple = new SourcedTuple(new BaseTuple());
		tuple.setBoolean("booleanField", booleanValue);
		tuple.setInt("intField", intValue);
		tuple.setLong("longField", longValue);
		tuple.setSource(2);
		return tuple;
	}

	@Test
	public void testObjectComparison() throws CoGrouperException, JsonGenerationException, JsonMappingException, IOException, InvalidFieldException {
		SortComparator comparator = new SortComparator();
		Configuration conf = new Configuration();
		
		PangoolConfig config = new PangoolConfigBuilder()
			.setGroupByFields("booleanField", "intField")
			.setSorting(new SortingBuilder()
				.add("booleanField", SortOrder.ASC)
				.add("intField", SortOrder.DESC)
				.addSourceId(SortOrder.ASC)
				.secondarySort(1).add("strField", SortOrder.DESC)
				.secondarySort(2).add("longField", SortOrder.ASC)
				.buildSorting()
			)
			.addSchema(1, Schema.parse("booleanField:boolean, intField:int, strField:string"))
			.addSchema(2, Schema.parse("booleanField:boolean, intField:int, longField:long"))
			.build();
		PangoolConfig.setPangoolConfig(config, conf);
		
		comparator.setConf(conf);
		Assert.assertEquals(0, comparator.compare(getTuple1(true, 10, "a"), getTuple1(true, 10, "a")));
		Assert.assertEquals(-1, comparator.compare(getTuple1(false, 10, "a"), getTuple1(true, 10, "a")));
		Assert.assertEquals(1, comparator.compare(getTuple1(true, 10, "a"), getTuple1(false, 10, "a")));
		Assert.assertEquals(1, comparator.compare(getTuple1(true, 1, "a"), getTuple1(true, 10, "a")));
		Assert.assertEquals(-1, comparator.compare(getTuple1(true, 10, "a"), getTuple1(true, 1, "a")));
		Assert.assertEquals(-1, comparator.compare(getTuple1(true, 10, "b"), getTuple1(true, 1, "a")));
		Assert.assertEquals(1, comparator.compare(getTuple1(true, 10, "c"), getTuple1(true, 1, "b")));
	}
}

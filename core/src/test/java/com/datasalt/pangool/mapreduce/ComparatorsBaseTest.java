package com.datasalt.pangool.mapreduce;

import java.io.IOException;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;

import com.datasalt.pangool.CoGrouperConfig;
import com.datasalt.pangool.CoGrouperConfigBuilder;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.SortCriteria.SortOrder;
import com.datasalt.pangool.SortingBuilder;
import com.datasalt.pangool.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangool.io.tuple.Tuple;

public abstract class ComparatorsBaseTest {

	public final static int SOURCE1 = 1;
	public final static int SOURCE2 = 2;
	
	protected void setConf(SortComparator comparator) throws InvalidFieldException, CoGrouperException, JsonGenerationException, JsonMappingException, IOException, InvalidFieldException {
		Configuration conf = new Configuration();
		CoGrouperConfig config = new CoGrouperConfigBuilder()
		.setGroupByFields("booleanField", "intField")
		.setSorting(new SortingBuilder()
			.add("booleanField", SortOrder.ASC)
			.add("intField", SortOrder.DESC)
			.addSourceId(SortOrder.DESC)
			.secondarySort(1).add("strField", SortOrder.DESC)
			.secondarySort(2).add("longField", SortOrder.ASC)
			.buildSorting()
		)
		.addSchema(SOURCE1, Schema.parse("booleanField:boolean, intField:int, strField:string"))
		.addSchema(SOURCE2, Schema.parse("booleanField:boolean, intField:int, longField:long"))
		.build();
		CoGrouperConfig.setPangoolConfig(config, conf);
		comparator.setConf(conf);
	}
	
	protected Tuple getTuple1(boolean booleanValue, int intValue, String strValue) {
		Tuple tuple = new Tuple(4);
		tuple.setBoolean(0, booleanValue);
		tuple.setInt(1, intValue);
		tuple.setInt(2, SOURCE1);
		tuple.setString(3, Utf8.getBytesFor(strValue));
		return tuple;
	}
	
	protected Tuple getTuple2(boolean booleanValue, int intValue, long longValue) {
		Tuple tuple = new Tuple(4);
		tuple.setBoolean(0, booleanValue);
		tuple.setInt(1, intValue);
		tuple.setInt(2, SOURCE2);
		tuple.setLong(3, longValue);
		return tuple;
	}
}

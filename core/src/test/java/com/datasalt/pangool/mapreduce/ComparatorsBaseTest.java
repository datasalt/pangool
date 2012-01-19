package com.datasalt.pangool.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;

import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.Tuple;
import com.datasalt.pangool.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangool.mapreduce.SortComparator;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.PangoolConfig;
import com.datasalt.pangool.PangoolConfigBuilder;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.SortingBuilder;
import com.datasalt.pangool.Schema.Field;
import com.datasalt.pangool.SortCriteria.SortOrder;

public class ComparatorsBaseTest {

	protected void setConf(SortComparator comparator) throws InvalidFieldException, CoGrouperException, JsonGenerationException, JsonMappingException, IOException, InvalidFieldException {
		Configuration conf = new Configuration();
		PangoolConfig config = new PangoolConfigBuilder()
		.setGroupByFields("booleanField", "intField")
		.setSorting(new SortingBuilder()
			.add("booleanField", SortOrder.ASC)
			.add("intField", SortOrder.DESC)
			.addSourceId(SortOrder.DESC)
			.secondarySort(1).add("strField", SortOrder.DESC)
			.secondarySort(2).add("longField", SortOrder.ASC)
			.buildSorting()
		)
		.addSchema(1, Schema.parse("booleanField:boolean, intField:int, strField:string"))
		.addSchema(2, Schema.parse("booleanField:boolean, intField:int, longField:long"))
		.build();
		PangoolConfig.setPangoolConfig(config, conf);
		comparator.setConf(conf);
	}
	
	protected Tuple getTuple1(boolean booleanValue, int intValue, String strValue) {
		Tuple tuple = new Tuple();
		tuple.setBoolean("booleanField", booleanValue);
		tuple.setInt("intField", intValue);
		tuple.setString("strField", strValue);
		tuple.setInt(Field.SOURCE_ID_FIELD_NAME, 1);
		return tuple;
	}
	
	protected Tuple getTuple2(boolean booleanValue, int intValue, long longValue) {
		Tuple tuple = new Tuple();
		tuple.setBoolean("booleanField", booleanValue);
		tuple.setInt("intField", intValue);
		tuple.setLong("longField", longValue);
		tuple.setInt(Field.SOURCE_ID_FIELD_NAME, 2);
		return tuple;
	}
}

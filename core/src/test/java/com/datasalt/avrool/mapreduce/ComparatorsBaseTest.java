package com.datasalt.avrool.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;

import com.datasalt.avrool.CoGrouperConfig;
import com.datasalt.avrool.CoGrouperConfigBuilder;
import com.datasalt.avrool.CoGrouperException;
import com.datasalt.avrool.Schema;
import com.datasalt.avrool.SortingBuilder;
import com.datasalt.avrool.Schema.Field;
import com.datasalt.avrool.SortCriteria.SortOrder;
import com.datasalt.avrool.io.tuple.ITuple;
import com.datasalt.avrool.io.tuple.Tuple;
import com.datasalt.avrool.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.avrool.mapreduce.SortComparator;

public abstract class ComparatorsBaseTest {

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
		.addSchema(1, Schema.parse("booleanField:boolean, intField:int, strField:string"))
		.addSchema(2, Schema.parse("booleanField:boolean, intField:int, longField:long"))
		.build();
		CoGrouperConfig.setPangoolConfig(config, conf);
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

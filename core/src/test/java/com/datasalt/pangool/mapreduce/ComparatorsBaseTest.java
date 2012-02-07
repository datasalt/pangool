package com.datasalt.pangool.mapreduce;

import java.io.IOException;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.junit.Before;

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
	
	private Schema schema1;
	private Schema schema2;
	
	@Before
	public void initSchemas() throws CoGrouperException{
		this.schema1 =  Schema.parse("booleanField:boolean, intField:int, strField:string");
		this.schema2 = Schema.parse("booleanField:boolean, intField:int, longField:long");
		
	}
	
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
		.addSchema(SOURCE1, schema1)
		.addSchema(SOURCE2, schema2)
		.build();
		CoGrouperConfig.set(config, conf);
		comparator.setConf(conf);
	}
	
	protected Tuple getTuple1(boolean booleanValue, int intValue, String strValue) {
		Tuple tuple = new Tuple(schema1);
		tuple.setBoolean("booleanField", booleanValue);
		tuple.setInt("intField", intValue);
		tuple.setString("strField", strValue);
		return tuple;
	}
	
	protected Tuple getTuple2(boolean booleanValue, int intValue, long longValue) {
		Tuple tuple = new Tuple(schema2);
		tuple.setBoolean("booleanField", booleanValue);
		tuple.setInt("intField", intValue);
		tuple.setLong("longField", longValue);
		return tuple;
	}
}

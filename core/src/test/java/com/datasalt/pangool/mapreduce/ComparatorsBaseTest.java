package com.datasalt.pangool.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.junit.Assert;
import org.junit.Before;

import com.datasalt.pangool.CoGrouperConfig;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.ConfigBuilder;
import com.datasalt.pangool.Criteria.Order;
import com.datasalt.pangool.Fields;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.SortBy;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangool.io.tuple.Tuple;

public abstract class ComparatorsBaseTest {
	
	private Schema schema1;
	private Schema schema2;
	
	@Before
	public void initSchemas() throws CoGrouperException{
		this.schema1 =  new Schema("schema1",Fields.parse("intField:int, strField:string,booleanField:boolean"));
		this.schema2 = new Schema("schema2",Fields.parse("longField:long,booleanField:boolean, intField:int"));
		
	}
	
	protected void setConf(SortComparator comparator) throws InvalidFieldException, CoGrouperException, JsonGenerationException, JsonMappingException, IOException, InvalidFieldException {
		
		Configuration conf = new Configuration();
		ConfigBuilder b = new ConfigBuilder();
		b.addSourceSchema(schema1);
		b.addSourceSchema(schema2);
		b.setGroupByFields("booleanField", "intField");
		b.setOrderBy(new SortBy().add("booleanField",Order.ASC).add("intField",Order.DESC).addSourceOrder(Order.DESC));
		b.setSecondaryOrderBy("schema1",new SortBy().add("strField",Order.DESC));
		b.setSecondaryOrderBy("schema2",new SortBy().add("longField",Order.DESC));
		CoGrouperConfig config = b.buildConf();
		CoGrouperConfig.set(config, conf);
		comparator.setConf(conf);
	}
	
	protected Tuple getTuple1(boolean booleanValue, int intValue, String strValue) {
		Tuple tuple = new Tuple(schema1);
		tuple.set("booleanField", booleanValue);
		tuple.set("intField", intValue);
		tuple.set("strField", strValue);
		return tuple;
	}
	
	protected Tuple getTuple2(boolean booleanValue, int intValue, long longValue) {
		Tuple tuple = new Tuple(schema2);
		tuple.set("booleanField", booleanValue);
		tuple.set("intField", intValue);
		tuple.set("longField", longValue);
		return tuple;
	}
	
	protected static void assertPositive(RawComparator comp,ITuple t1,ITuple t2){
		assertPositive(comp.compare(t1,t2));
	}
	
	protected static void assertNegative(RawComparator comp,ITuple t1,ITuple t2){
		assertNegative(comp.compare(t1,t2));
	}
	
	protected static void assertPositive(int n){
		Assert.assertTrue(n > 0);
	}
	
	protected static void assertNegative(int n){
		Assert.assertTrue(n < 0);
	}

}

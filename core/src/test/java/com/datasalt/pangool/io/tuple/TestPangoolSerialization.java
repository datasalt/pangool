package com.datasalt.pangool.io.tuple;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.datasalt.pangolin.thrift.test.A;
import com.datasalt.pangool.BaseTest;
import com.datasalt.pangool.CoGrouperConfig;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.ConfigBuilder;
import com.datasalt.pangool.Criteria.Order;
import com.datasalt.pangool.Fields;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.SortBy;


public class TestPangoolSerialization extends BaseTest{

	protected CoGrouperConfig pangoolConf;
	
	public static enum TestEnum {
		A,B,C
	};
	
	@Before
	public void prepare2() throws CoGrouperException{
		ConfigBuilder b = new ConfigBuilder();
		b.addSourceSchema(new Schema("schema1",Fields.parse("booleanField:boolean, intField:int, strField:string")));
		b.addSourceSchema(new Schema("schema2",Fields.parse("booleanField:boolean, intField:int, longField:long")));
		b.addSourceSchema(new Schema("schema3",Fields.parse("booleanField:boolean, intField:int, longField:long, vlongField:vlong,vintField:vint,strField:string")));
		b.addSourceSchema(new Schema("schema4",Fields.parse("booleanField:boolean, intField:int, longField:long, vlongField:vlong,vintField:vint,strField:string")));
		b.addSourceSchema(new Schema("schema5",Fields.parse("booleanField:boolean, intField:int, longField:long, vlongField:vlong,vintField:vint,strField:string, enumField:"+TestEnum.class.getName() + ",thriftField:" + A.class.getName())));
		
		b.setGroupByFields("booleanField","intField");
		b.setOrderBy(new SortBy().add("booleanField",Order.ASC).add("intField",Order.DESC));
		b.setSecondaryOrderBy("schema1",new SortBy().add("strField",Order.DESC));
		b.setSecondaryOrderBy("schema2",new SortBy().add("longField",Order.ASC));
		pangoolConf = b.buildConf();
	}
	
	@Test
	public void testRandomTupleSerialization() throws IOException,  CoGrouperException {
		CoGrouperConfig.set(pangoolConf, getConf());
		Tuple tuple = new Tuple(SCHEMA);
		int NUM_ITERATIONS=100000;
		for (int i=0 ; i < NUM_ITERATIONS; i++){
			fillTuple(true,tuple);
			//assertSerializable(ser, tuple,true);
		}
		
	}
	
}

package com.datasalt.pangool.io.tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import com.datasalt.pangool.io.tuple.Tuple;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangolin.thrift.test.A;
import com.datasalt.pangool.BaseTest;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.PangoolConfig;
import com.datasalt.pangool.PangoolConfigBuilder;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.Schema.Field;
import com.datasalt.pangool.SortCriteria.SortOrder;
import com.datasalt.pangool.SortingBuilder;

public class TestSourcedTuple extends BaseTest{

	PangoolConfig pangoolConf;
	
	public static enum TestEnum {
		A,B,C
	};
	
	@Before
	public void prepare2() throws InvalidFieldException, CoGrouperException{
		pangoolConf = new PangoolConfigBuilder()
		.setGroupByFields("booleanField", "intField")
		.setSorting(new SortingBuilder().add("booleanField", SortOrder.ASC)
			.add("intField", SortOrder.DESC)
			.addSourceId(SortOrder.ASC)
			.secondarySort(1).add("strField", SortOrder.DESC)
			.secondarySort(2).add("longField", SortOrder.ASC)
			.buildSorting()
		)
		.addSchema(1, Schema.parse("booleanField:boolean, intField:int, strField:string"))
		.addSchema(2, Schema.parse("booleanField:boolean, intField:int, longField:long"))
		.addSchema(3, Schema.parse("booleanField:boolean, intField:int, longField:long, vlongField:vlong,vintField:vint,strField:string"))
		.addSchema(4, Schema.parse("booleanField:boolean,intField:int, longField:long, vlongField:vlong,vintField:vint,strField:string"))
		.addSchema(5, Schema.parse("booleanField:boolean,intField:int, longField:long, vlongField:vlong,vintField:vint,strField:string,enumField:"+TestEnum.class.getName() + ",thriftField:" + A.class.getName()))
		.build();
	}
	
	
	
	@Test
	public void testRandomTupleSerialization() throws IOException, InvalidFieldException, CoGrouperException {
		
		
		
		PangoolConfig.setPangoolConfig(pangoolConf, getConf());
		
			Random random = new Random();
			
			int NUM_ITERATIONS=100000;
			
			List<Integer> sourceIds = new ArrayList<Integer>(pangoolConf.getSchemes().keySet());
			Tuple baseTuple = new Tuple();
			ITuple dbTuple = new DoubleBufferedTuple();
			ITuple[] tuples = new ITuple[]{baseTuple,dbTuple};
			for (int i=0 ; i < NUM_ITERATIONS; i++){
				int sourceId = sourceIds.get(random.nextInt(sourceIds.size()));
				for (ITuple tuple : tuples){
					tuple.clear();
					tuple.setInt(Field.SOURCE_ID_FIELD_NAME, sourceId);
					Schema schema = pangoolConf.getSchemaBySourceId(sourceId);
					fillTuple(true,schema, tuple, 0, schema.getFields().size()-1);
					System.out.println(tuple);
					assertSerializable(tuple,true);
				}
			
		}
	}
	
}

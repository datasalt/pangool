package com.datasalt.avrool.io.tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import com.datasalt.avrool.BaseTest;
import com.datasalt.avrool.CoGrouperConfig;
import com.datasalt.avrool.CoGrouperConfigBuilder;
import com.datasalt.avrool.CoGrouperException;
import com.datasalt.avrool.Schema;
import com.datasalt.avrool.SortingBuilder;
import com.datasalt.avrool.Schema.Field;
import com.datasalt.avrool.SortCriteria.SortOrder;
import com.datasalt.avrool.io.Serialization;
import com.datasalt.avrool.io.tuple.DoubleBufferedTuple;
import com.datasalt.avrool.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.avrool.thrift.test.A;

public class TestTupleInternalSerialization extends BaseTest{

	CoGrouperConfig pangoolConf;
	
	public static enum TestEnum {
		A,B,C
	};
	
	@Before
	public void prepare2() throws InvalidFieldException, CoGrouperException{
		pangoolConf = new CoGrouperConfigBuilder()
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
		CoGrouperConfig.setPangoolConfig(pangoolConf, getConf());
		Serialization ser = new Serialization(getConf());
			Random random = new Random();
			int NUM_ITERATIONS=100000;
			List<Integer> sourceIds = new ArrayList<Integer>(pangoolConf.getSchemes().keySet());
			DoubleBufferedTuple dbTuple = new DoubleBufferedTuple();
			DoubleBufferedTuple[] tuples = new DoubleBufferedTuple[]{dbTuple};
			for (int i=0 ; i < NUM_ITERATIONS; i++){
				int sourceId = sourceIds.get(random.nextInt(sourceIds.size()));
				for (DoubleBufferedTuple tuple : tuples){
					tuple.clear();
					tuple.setInt(Field.SOURCE_ID_FIELD_NAME, sourceId);
					Schema schema = pangoolConf.getSchemaBySourceId(sourceId);
					fillTuple(true,schema, tuple, 0, schema.getFields().size()-1);
					assertSerializable(ser,tuple,false);
				}
		}
	}
	
}

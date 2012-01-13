package com.datasalt.pangool.io.tuple;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Test;

import com.datasalt.pangolin.grouper.io.tuple.BaseTuple;
import com.datasalt.pangolin.grouper.io.tuple.ITuple;
import com.datasalt.pangolin.grouper.io.tuple.Tuple;
import com.datasalt.pangolin.grouper.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangolin.io.Serialization;
import com.datasalt.pangool.BaseTest;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.PangoolConfig;
import com.datasalt.pangool.PangoolConfigBuilder;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.SortCriteria.SortOrder;
import com.datasalt.pangool.SortingBuilder;

public class TestSourcedTuple extends BaseTest{

	enum TestEnum {
		S,Blabla
	};
	
	
	@Test
	public void testRandomTupleSerialization() throws IOException, InvalidFieldException, CoGrouperException {
		
		PangoolConfig pangoolConf = new PangoolConfigBuilder()
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
		.build();
		
		PangoolConfig.setPangoolConfig(pangoolConf, getConf());
		
			Random random = new Random();
			SourcedTuple tuple = new SourcedTuple(new BaseTuple());
			int NUM_ITERATIONS=10000;
			//Set<Integer> sourceIds = pangoolConf.getSchemes().keySet();
			List<Integer> sourceIds = new ArrayList<Integer>(pangoolConf.getSchemes().keySet());
			
			for (int i=0 ; i < NUM_ITERATIONS; i++){
				int sourceId = sourceIds.get(random.nextInt(sourceIds.size()));
				tuple.setSource(sourceId);
				
				Schema schema = pangoolConf.getSchemaBySourceId(sourceId);
				fillTuple(true,schema, tuple, 0, schema.getFields().size()-1);
				System.out.println(tuple);
				assertSerializable(tuple,false);
//				assertSerializable2(tuple,false);
			
		}
	}
	
	
	


}

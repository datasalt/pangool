/**
 * Copyright [2012] [Datasalt Systems S.L.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasalt.pangool.tuplemr.serialization;

import java.io.IOException;

import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.datasalt.pangool.BaseTest;
import com.datasalt.pangool.io.DatumWrapper;
import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.serialization.HadoopSerialization;
import com.datasalt.pangool.serialization.ThriftSerialization;
import com.datasalt.pangool.thrift.test.A;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.TupleMRConfig;
import com.datasalt.pangool.tuplemr.TupleMRConfigBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.utils.AvroUtils;
import com.datasalt.pangool.utils.AvroRecordToTupleConverter;
import com.datasalt.pangool.utils.TupleToAvroRecordConverter;


public class TestTupleSerialization extends BaseTest{

	protected TupleMRConfig pangoolConf;
	
	public static enum TestEnum {
		A,B,C
	};
	
	@Before
	public void prepare2() throws TupleMRException{
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",
				Fields.parse("boolean_field:boolean, int_field:int, string_field:string")));
		b.addIntermediateSchema(new Schema("schema2",
				Fields.parse("boolean_field:boolean, int_field:int, long_field:long")));
		b.addIntermediateSchema(new Schema("schema3",
				Fields.parse("boolean_field:boolean, int_field:int, long_field:long,strField:string")));
		b.addIntermediateSchema(new Schema("schema4",
				Fields.parse("boolean_field:boolean, int_field:int, long_field:long,strField:string")));
		b.addIntermediateSchema(new Schema("schema5",
				Fields.parse("boolean_field:boolean, int_field:int, long_field:long,strField:string, " +
						"enum_field:"+TestEnum.class.getName() + ",thrift_field:" + A.class.getName())));
		b.addIntermediateSchema(SCHEMA);
		b.setGroupByFields("boolean_field","int_field");
		b.setOrderBy(new OrderBy().add("boolean_field",Order.ASC).add("int_field",Order.DESC).addSchemaOrder(Order.DESC));
		b.setSpecificOrderBy("schema1",new OrderBy().add("string_field",Order.DESC));
		b.setSpecificOrderBy("schema2",new OrderBy().add("long_field",Order.ASC));
		pangoolConf = b.buildConf();
	}
	
	@Test
	public void testRandomTupleSerialization() throws IOException,  TupleMRException {
		Configuration conf = getConf();
		//ThriftSerialization.enableThriftSerialization(conf);
		
		HadoopSerialization hadoopSer = new HadoopSerialization(conf);
		//defined in BaseTest
		Schema schema = pangoolConf.getIntermediateSchema("schema"); //most complete
		
		TupleSerialization serialization = new TupleSerialization(hadoopSer,pangoolConf);
		
		TupleSerializer serializer = (TupleSerializer)serialization.getSerializer(null);
		TupleDeserializer deser = (TupleDeserializer)serialization.getDeserializer(null);
		Tuple tuple = new Tuple(schema);
		int NUM_ITERATIONS=100000;
		DatumWrapper<ITuple> wrapper = new DatumWrapper<ITuple>(tuple);
		for (int i=0 ; i < NUM_ITERATIONS; i++){
			fillTuple(true,wrapper.datum());
			assertSerializable(serializer, deser, wrapper, false);
		}
	}
	
	@Test
	public void testTupleToRecordConversion() throws Exception {
		Schema schema = SCHEMA; //TODO add permutations of this schema
		Configuration conf = getConf();
		//ThriftSerialization.enableThriftSerialization(conf);
		
		TupleToAvroRecordConverter pangoolToAvro = 
				new TupleToAvroRecordConverter(schema, conf);
		org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(schema);
		Schema convertedSchema = AvroUtils.toPangoolSchema(avroSchema);
		Assert.assertEquals(schema,convertedSchema);
		
		AvroRecordToTupleConverter avroToPangool = 
				new AvroRecordToTupleConverter(avroSchema, conf);
		
		ITuple tuple = new Tuple(schema);
		ITuple convertedTuple=null;
		Record record=null;
		int NUM_ITERATIONS=100000;
		for (int i=0 ; i < NUM_ITERATIONS; i++){
			fillTuple(true,tuple);
			record = pangoolToAvro.toRecord(tuple,record);
			
			convertedTuple=avroToPangool.toTuple(record,convertedTuple);
			Assert.assertEquals(tuple,convertedTuple);
		}
	}
}

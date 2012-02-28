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
package com.datasalt.pangool.tuplemr.tuplemr.serialization;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
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
import com.datasalt.pangool.tuplemr.SortBy;
import com.datasalt.pangool.tuplemr.TupleMRConfig;
import com.datasalt.pangool.tuplemr.TupleMRConfigBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.serialization.TupleDeserializer;
import com.datasalt.pangool.tuplemr.serialization.TupleSerialization;
import com.datasalt.pangool.tuplemr.serialization.TupleSerializer;


public class TestPangoolSerialization extends BaseTest{

	protected TupleMRConfig pangoolConf;
	
	public static enum TestEnum {
		A,B,C
	};
	
	@Before
	public void prepare2() throws TupleMRException{
		TupleMRConfigBuilder b = new TupleMRConfigBuilder();
		b.addIntermediateSchema(new Schema("schema1",Fields.parse("booleanField:boolean, intField:int, strField:utf8")));
		b.addIntermediateSchema(new Schema("schema2",Fields.parse("booleanField:boolean, intField:int, longField:long")));
		b.addIntermediateSchema(new Schema("schema3",Fields.parse("booleanField:boolean, intField:int, longField:long,strField:utf8")));
		b.addIntermediateSchema(new Schema("schema4",Fields.parse("booleanField:boolean, intField:int, longField:long,strField:utf8")));
		b.addIntermediateSchema(new Schema("schema5",Fields.parse("booleanField:boolean, intField:int, longField:long,strField:utf8, enumField:"+TestEnum.class.getName() + ",thriftField:" + A.class.getName())));
		
		b.setGroupByFields("booleanField","intField");
		b.setOrderBy(new SortBy().add("booleanField",Order.ASC).add("intField",Order.DESC).addSourceOrder(Order.DESC));
		b.setSecondaryOrderBy("schema1",new SortBy().add("strField",Order.DESC));
		b.setSecondaryOrderBy("schema2",new SortBy().add("longField",Order.ASC));
		pangoolConf = b.buildConf();
	}
	
	@Test
	public void testRandomTupleSerialization() throws IOException,  TupleMRException {
		Configuration conf = new Configuration();
		ThriftSerialization.enableThriftSerialization(conf);
		
		HadoopSerialization hadoopSer = new HadoopSerialization(conf);
		Schema schema = pangoolConf.getIntermediateSchema("schema5"); //most complete
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
	
}

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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Test;

import cern.colt.Arrays;

import com.datasalt.pangool.io.DatumWrapper;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.io.Utf8;
import com.datasalt.pangool.serialization.HadoopSerialization;
import com.datasalt.pangool.thrift.test.A;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.TupleMRConfig;
import com.datasalt.pangool.tuplemr.TupleMRConfigBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.mapred.BaseComparator;
import com.datasalt.pangool.tuplemr.mapred.SortComparator;
import com.datasalt.pangool.utils.test.AbstractBaseTest;

/**
 * TODO: Build a proper test. 
 */
@SuppressWarnings("serial")
public class TestSingleFieldDeserializer extends AbstractBaseTest implements Serializable {

	@Test
	public void testThrift() throws IOException, TupleMRException {
		Configuration conf = getConf();
		
		ArrayList<Field> fields = new ArrayList<Field> ();
		Field field = Field.createObject("a", A.class);
		fields.add(field);
		Schema schema = new Schema("schema", fields);

		Tuple tuple1 = new Tuple(schema);
		final A a = new A("hola", "colega");
		tuple1.set("a", a);

		final A b = new A("bloblo","coco");
		Tuple tuple2 = new Tuple(schema);
		tuple2.set("a", b);
		
		TupleMRConfigBuilder builder = new TupleMRConfigBuilder();
		builder.addIntermediateSchema(schema);
		builder.setGroupByFields("a");
		builder.setOrderBy(new OrderBy().add("a", Order.ASC, new BaseComparator<A>(Type.OBJECT,A.class) {

			@Override
      public int compare(A o1, A o2) {
				assertEquals(a, o1);
				assertEquals(b, o2);
				return 1;
      }
			
		}));		
		
		TupleMRConfig grouperConf = builder.buildConf();
		TupleMRConfig.set(grouperConf, conf);
		
		HadoopSerialization ser = new HadoopSerialization(conf);
	
		DataOutputBuffer buffer1 = new DataOutputBuffer();
		ser.ser(new DatumWrapper<ITuple>(tuple1), buffer1);

		SingleFieldDeserializer fieldDeser = new SingleFieldDeserializer(conf, grouperConf,field.getType(),field.getObjectClass());
		A otherA = (A) fieldDeser.deserialize(buffer1.getData(), 0);
		assertEquals(a, otherA);

		DataOutputBuffer buffer2 = new DataOutputBuffer();
		ser.ser(new DatumWrapper<ITuple>(tuple2), buffer2);
		
		SortComparator sortComparator = new SortComparator();
		sortComparator.setConf(conf);

		System.out.println("buff1: " + Arrays.toString(buffer1.getData()));
		System.out.println("buff2: " + Arrays.toString(buffer2.getData()));
		
		sortComparator.compare(buffer1.getData(), 0, buffer1.size(), buffer2.getData(), 0, buffer2.size());
			
	}
	
	@Test
	public void testInteger() throws IOException, TupleMRException {
		Configuration conf = getConf();
		
		ArrayList<Field> fields = new ArrayList<Field> ();
		Field field = Field.create("int",Type.INT);
		fields.add(field);
		Schema schema = new Schema("schema", fields);

		Tuple tuple1 = new Tuple(schema);
		tuple1.set("int", 200);

		Tuple tuple2 = new Tuple(schema);
		tuple2.set("int", -123);
		
		TupleMRConfigBuilder builder = new TupleMRConfigBuilder();
		builder.addIntermediateSchema(schema);
		builder.setGroupByFields("int");
		builder.setOrderBy(new OrderBy().add("int", Order.ASC, new BaseComparator<Integer>(Type.INT) {

			@Override
      public int compare(Integer o1, Integer o2) {
				assertEquals(200, (int) o1);
				assertEquals(-123,(int) o2);
				
				return 1;
      }
			
		}));		
		
		TupleMRConfig grouperConf = builder.buildConf();
		TupleMRConfig.set(grouperConf, conf);
		
		HadoopSerialization ser = new HadoopSerialization(conf);
	
		DataOutputBuffer buffer1 = new DataOutputBuffer();
		ser.ser(new DatumWrapper<ITuple>(tuple1), buffer1);

		SingleFieldDeserializer fieldDeser = new SingleFieldDeserializer(conf, grouperConf,field.getType(),field.getObjectClass());
		Integer iDeser = (Integer) fieldDeser.deserialize(buffer1.getData(), 0);
		assertEquals(200, (int) iDeser);

		DataOutputBuffer buffer2 = new DataOutputBuffer();
		ser.ser(new DatumWrapper<ITuple>(tuple2), buffer2);
		
		SortComparator sortComparator = new SortComparator();
		sortComparator.setConf(conf);

		System.out.println("buff1: " + Arrays.toString(buffer1.getData()));
		System.out.println("buff2: " + Arrays.toString(buffer2.getData()));
		
		sortComparator.compare(buffer1.getData(), 0, buffer1.size(), buffer2.getData(), 0, buffer2.size());
			
	}
	
	@Test
	public void testUtf8() throws IOException, TupleMRException {
		Configuration conf = getConf();
		
		ArrayList<Field> fields = new ArrayList<Field> ();
		Field field = Field.create("utf8",Type.STRING);
		fields.add(field);
		Schema schema = new Schema("schema", fields);

		Tuple tuple1 = new Tuple(schema);
		tuple1.set("utf8", "lameculos");

		Tuple tuple2 = new Tuple(schema);
		tuple2.set("utf8", "mojigata");
		
		TupleMRConfigBuilder builder = new TupleMRConfigBuilder();
		builder.addIntermediateSchema(schema);
		builder.setGroupByFields("utf8");
		builder.setOrderBy(new OrderBy().add("utf8", Order.ASC, new BaseComparator<Utf8>(Type.STRING) {

			@Override
      public int compare(Utf8 o1, Utf8 o2) {
				assertEquals("lameculos", o1 + "");
				assertEquals("mojigata",o2 + "");
				
				return 1;
      }
			
		}));		
		
		TupleMRConfig grouperConf = builder.buildConf();
		TupleMRConfig.set(grouperConf, conf);
		
		HadoopSerialization ser = new HadoopSerialization(conf);
		
		DataOutputBuffer buffer1 = new DataOutputBuffer();
		ser.ser(new DatumWrapper<ITuple>(tuple1), buffer1);

		SingleFieldDeserializer fieldDeser = new SingleFieldDeserializer(conf, grouperConf,field.getType(),field.getObjectClass());
		Utf8 objDeser = (Utf8) fieldDeser.deserialize(buffer1.getData(), 0);		
		assertEquals("lameculos", objDeser + "");

		DataOutputBuffer buffer2 = new DataOutputBuffer();
		ser.ser(new DatumWrapper<ITuple>(tuple2), buffer2);
		
		SortComparator sortComparator = new SortComparator();
		sortComparator.setConf(conf);

		System.out.println("buff1: " + Arrays.toString(buffer1.getData()));
		System.out.println("buff2: " + Arrays.toString(buffer2.getData()));
		
		sortComparator.compare(buffer1.getData(), 0, buffer1.size(), buffer2.getData(), 0, buffer2.size());
			
	}
	
}

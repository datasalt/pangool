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
package com.datasalt.pangool;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.util.ReflectionUtils;

import com.datasalt.pangolin.thrift.test.A;
import com.datasalt.pangool.cogroup.sorting.Criteria.Order;
import com.datasalt.pangool.io.Utf8;
import com.datasalt.pangool.io.tuple.DatumWrapper;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.Schema;
import com.datasalt.pangool.io.tuple.Schema.Field;
import com.datasalt.pangool.serialization.hadoop.HadoopSerialization;
import com.datasalt.pangool.serialization.tuples.PangoolDeserializer;
import com.datasalt.pangool.serialization.tuples.PangoolSerialization;
import com.datasalt.pangool.serialization.tuples.PangoolSerializer;
import com.datasalt.pangool.test.AbstractBaseTest;

@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class BaseTest extends AbstractBaseTest {

	public final static  Schema SCHEMA;
	
	static{
		List<Field> fields = new ArrayList<Field>();
		fields.add(new Field("int_field",Integer.class));
		fields.add(new Field("string_field",Utf8.class));
		fields.add(new Field("long_field",Long.class));
  	fields.add(new Field("float_field",Float.class));
		fields.add(new Field("double_field",Double.class));
		fields.add(new Field("boolean_field",Boolean.class));
  	fields.add(new Field("enum_field",Order.class));
		fields.add(new Field("thrift_field",A.class));
		SCHEMA = new Schema("schema",fields);
	}

	static Random random = new Random(1);
	
	protected static void fillTuple(boolean random,ITuple tuple){
		fillTuple(random,tuple,0,tuple.getSchema().getFields().size()-1);
	}
	
	/**
	 * Fills the fields specified by the range (minIndex, maxIndex) with random data.
	 * 
	 */

	protected static void fillTuple(boolean isRandom,ITuple tuple, int minIndex, int maxIndex) {
		try {
			for(int i = minIndex; i <= maxIndex; i++) {
				Field field = tuple.getSchema().getField(i);
				Class fieldType = field.getType();
				if(fieldType == Integer.class) {
					tuple.set(i, isRandom ? random.nextInt() : 0);
				} else if(fieldType == Long.class) {
					tuple.set(i, isRandom ? random.nextLong() : 0);
				} else if(fieldType == Boolean.class) {
					tuple.set(i, isRandom ? random.nextBoolean() : false);
				} else if(fieldType == Double.class) {
					tuple.set(i, isRandom ? random.nextDouble() : 0.0);
				} else if(fieldType == Float.class) {
					tuple.set(i, isRandom ? random.nextFloat() : 0f);
				} else if(fieldType == Utf8.class) {
					if (isRandom) {
						switch (random.nextInt(4)) {
						case 0:
							tuple.set(i, "");
							break;
						case 1:
							tuple.set(i, random.nextLong() + "");
							break;
						case 2:
							tuple.set(i, new Utf8(random.nextLong() + ""));
							break;
						case 3:
							tuple.set(i, new Text(random.nextLong() + ""));
							break;														
						}
					} else {
						tuple.set(i, "");
					}
				} else if(fieldType.isEnum()) {
          Method method = fieldType.getMethod("values", (Class[])null);
					Enum[] values = (Enum[]) method.invoke(null);
					tuple.set(i, values[isRandom ? random.nextInt(values.length) : 0]);
				} else {
					boolean toInstance = random.nextBoolean();
					if(isRandom && toInstance) {
						Object instance = ReflectionUtils.newInstance(fieldType, null);
						
						if (instance instanceof A) {
							
							A a = (A) instance;
							a.setId(random.nextInt() + "");
							a.setUrl(random.nextLong() + "");
						}
						tuple.set(i, instance);
					} else {
						tuple.set(i, null);
					}
				}
			}
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
	}

	protected static void assertSerializable(HadoopSerialization ser,ITuple tuple, boolean debug) throws IOException {
		DataInputBuffer input = new DataInputBuffer();
		DataOutputBuffer output = new DataOutputBuffer();
		DatumWrapper<ITuple> wrapper = new DatumWrapper<ITuple>(tuple);
		ser.ser(wrapper, output);

		input.reset(output.getData(), 0, output.getLength());
		DatumWrapper<ITuple> wrapper2 = new DatumWrapper<ITuple>();
		
		wrapper2 = ser.deser(wrapper2, input);
		if(debug) {
			System.out.println("D:" + wrapper2.datum());
		}
		assertEquals(tuple, wrapper2.datum());
	}
	
//	protected static void assertSerializable(CoGrouperConfig config,ITuple tuple,boolean debug) throws IOException {
//		//HadoopSerialization hadoopSerialization = new HadoopSerialization(conf)
//		PangoolSerialization serialization = new PangoolSerialization();
//	}
	
	protected static void assertSerializable(PangoolSerialization serialization,DatumWrapper<ITuple> tuple,boolean debug) throws IOException {
		PangoolSerializer ser = (PangoolSerializer)serialization.getSerializer(null); 
		PangoolDeserializer deser = (PangoolDeserializer)serialization.getDeserializer(null); 
		assertSerializable(ser,deser,tuple,debug);
	}
	
	protected static void assertSerializable(PangoolSerializer ser,PangoolDeserializer deser,DatumWrapper<ITuple> tuple,boolean debug) throws IOException {
		
		DataOutputBuffer output = new DataOutputBuffer();
		ser.open(output);
		ser.serialize(tuple);
		ser.close();

		DataInputBuffer input = new DataInputBuffer();
		input.reset(output.getData(), 0, output.getLength());
		DatumWrapper<ITuple> deserializedTuple = new DatumWrapper<ITuple>();
		
		deser.open(input);
		deserializedTuple = deser.deserialize(deserializedTuple);
		deser.close();
		
		if(debug) {
			System.out.println("D:" + deserializedTuple.datum());
		}
		assertEquals(tuple.datum(), deserializedTuple.datum());
	}

}

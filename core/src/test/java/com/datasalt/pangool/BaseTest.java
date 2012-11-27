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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

import com.datasalt.pangool.io.DatumWrapper;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.io.Utf8;
import com.datasalt.pangool.serialization.HadoopSerialization;
import com.datasalt.pangool.thrift.test.A;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.serialization.AvroFieldSerialization;
import com.datasalt.pangool.tuplemr.serialization.TupleDeserializer;
import com.datasalt.pangool.tuplemr.serialization.TupleSerialization;
import com.datasalt.pangool.tuplemr.serialization.TupleSerializer;
import com.datasalt.pangool.utils.test.AbstractHadoopTestLibrary;

@SuppressWarnings({ "rawtypes" })
public abstract class BaseTest extends AbstractHadoopTestLibrary {

	public final static  Schema SCHEMA;
	public final static org.apache.avro.Schema AVRO_SCHEMA;
	
	static{
		List<Field> fields = new ArrayList<Field>();
		fields.add(Field.create("int_field",Type.INT));
		fields.add(Field.create("string_field",Type.STRING));
		fields.add(Field.create("long_field",Type.LONG));
  	fields.add(Field.create("float_field",Type.FLOAT));
  	fields.add(Field.create("bytes_field",Type.BYTES));
		fields.add(Field.create("double_field",Type.DOUBLE));
		fields.add(Field.create("boolean_field",Type.BOOLEAN));
  	fields.add(Field.createEnum("enum_field",Order.class));
		fields.add(Field.createObject("thrift_field",A.class));
		
		AVRO_SCHEMA =  org.apache.avro.Schema.createRecord("MyRecordSchema",null,null,false);
		List<org.apache.avro.Schema.Field> avroFields = new ArrayList<org.apache.avro.Schema.Field>();
		avroFields.add(new org.apache.avro.Schema.Field
				("my_int",org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT),null,null));
		avroFields.add(new org.apache.avro.Schema.Field
				("my_string",org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),null,null));	
		AVRO_SCHEMA.setFields(avroFields);
		Field avroField =Field.createObject("my_avro",Object.class);
		avroField.setObjectSerialization(AvroFieldSerialization.class);
		avroField.addProp("avro.schema",AVRO_SCHEMA.toString());
		fields.add(avroField);
		SCHEMA = new Schema("schema",fields);
	}

	static Random random = new Random(1);
	
	protected static ITuple fillTuple(boolean random,ITuple tuple){
		return fillTuple(random,tuple,0,tuple.getSchema().getFields().size()-1);
	}
	
	/**
	 * Fills the fields specified by the range (minIndex, maxIndex) with random data.
	 * 
	 */

	protected static ITuple fillTuple(boolean isRandom,ITuple tuple, int minIndex, int maxIndex) {
		try {
			for(int i = minIndex; i <= maxIndex; i++) {
				Field field = tuple.getSchema().getField(i);
				switch(field.getType()){
					case INT:	tuple.set(i, isRandom ? random.nextInt() : 0);break;
					case LONG: tuple.set(i, isRandom ? random.nextLong() : 0);break;
					case BOOLEAN:	tuple.set(i, isRandom ? random.nextBoolean() : false); break;
					case DOUBLE: tuple.set(i, isRandom ? random.nextDouble() : 0.0); break;
					case FLOAT:	tuple.set(i, isRandom ? random.nextFloat() : 0f); break;
					case STRING: fillString(isRandom,tuple,i); break;
					case ENUM: fillEnum(isRandom,field,tuple,i); break;
					case OBJECT: fillObject(isRandom,tuple,field,i); break;
					case BYTES: fillBytes(isRandom,tuple,i); break;
					default: throw new IllegalArgumentException("Not supported type " + field.getType());
				}
			}
      return tuple;
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
	}
	protected static void fillString(boolean isRandom,ITuple tuple,int index){
		if (isRandom) {
			switch (random.nextInt(4)) {
			case 0:	tuple.set(index, "");	break;
			case 1:	tuple.set(index, random.nextLong() + "");	break;
			case 2:	tuple.set(index, new Utf8(random.nextLong() + ""));	break;
			case 3:	tuple.set(index, new Text(random.nextLong() + ""));	break;														
			}
		} else {
			tuple.set(index, "");
		}
	}
	protected static void fillEnum(boolean isRandom,Field field,ITuple tuple,int index) throws Exception{
		Enum[] values = (Enum[]) field.getObjectClass().getEnumConstants();
		tuple.set(index, values[isRandom ? random.nextInt(values.length) : 0]);
	}
	
	protected static void fillBytes(boolean isRandom,ITuple tuple,int index) throws Exception {
		Random random = new Random();
		int BYTES_SIZE=8;
		Object ob = tuple.get(index);
		if (ob == null || !(ob instanceof ByteBuffer)){
			ob = ByteBuffer.allocate(BYTES_SIZE);
			tuple.set(index,ob);
		}
		ByteBuffer buffer = (ByteBuffer)ob;
		if (isRandom){
			random.nextBytes(buffer.array());
			int newLimit = random.nextInt(buffer.capacity());
			buffer.limit(newLimit);
		} else {
			int newLimit=0;
			buffer.limit(newLimit);
		}
		
	}
	
	protected static void fillObject(boolean isRandom,ITuple tuple,Field field,int index){
		Object instance = tuple.get(index);
		if (field.getObjectClass() == A.class) {
			if (instance == null){
				instance = ReflectionUtils.newInstance(field.getObjectClass(), null);
			}
			A a = (A) instance;
			a.setId(isRandom ? random.nextInt() + "" : "");
			a.setUrl(isRandom ? random.nextLong() + "" : "");
		} else if (field.getObjectSerialization() == AvroFieldSerialization.class){
			if (instance == null || !(instance instanceof Record)){
				instance = new Record(AVRO_SCHEMA);
			}
			Record record = (Record)instance;
			record.put("my_int",isRandom ? random.nextInt() : 0);
			record.put("my_string",isRandom ? random.nextLong()+"" : "");
		} else {
			throw new PangoolRuntimeException("Unknown field to fill");
		}
		tuple.set(index, instance);
	}
	

	protected static void assertSerializable(HadoopSerialization ser,ITuple tuple, 
			boolean debug) throws IOException {
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
	
	protected static void assertSerializable(TupleSerialization serialization,
			DatumWrapper<ITuple> tuple,boolean debug) throws IOException {
		TupleSerializer ser = (TupleSerializer)serialization.getSerializer(null); 
		TupleDeserializer deser = (TupleDeserializer)serialization.getDeserializer(null); 
		assertSerializable(ser,deser,tuple,debug);
	}
	
	protected static void assertSerializable(TupleSerializer ser,
			TupleDeserializer deser,DatumWrapper<ITuple> tuple,boolean debug) throws IOException {
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

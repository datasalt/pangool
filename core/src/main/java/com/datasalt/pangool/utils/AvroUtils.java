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
package com.datasalt.pangool.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroSerialization;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

import com.datasalt.pangool.PangoolRuntimeException;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.Schema;
import com.datasalt.pangool.io.tuple.Schema.Field;
import com.datasalt.pangool.io.tuple.Schema.Field.Type;
import com.datasalt.pangool.serialization.hadoop.HadoopSerialization;

public class AvroUtils {

	public static void addAvroSerialization(Configuration conf) {
		Collection<String> serializations = conf.getStringCollection("io.serializations");
		if(!serializations.contains(AvroSerialization.class.getName())) {
			serializations.add(AvroSerialization.class.getName());
			conf.setStrings("io.serializations", serializations.toArray(new String[0]));
		}
	}

	/**
	 * Converts from one Avro schema to one Pangool schema for de-serializing it
	 */
	public static Schema toPangoolSchema(org.apache.avro.Schema avroSchema) {
		List<Field> fields = new ArrayList<Field>();
		for(org.apache.avro.Schema.Field field : avroSchema.getFields()) {
			org.apache.avro.Schema.Type type = field.schema().getType();
			switch(type){
			case INT: fields.add(Field.create(field.name(),Type.INT)); break;
			case FLOAT: fields.add(Field.create(field.name(),Type.FLOAT)); break;
			case DOUBLE: fields.add(Field.create(field.name(),Type.DOUBLE)); break;
			case BOOLEAN: fields.add(Field.create(field.name(),Type.BOOLEAN)); break;
			case STRING: fields.add(Field.create(field.name(),Type.STRING)); break;
			case BYTES:
				String clazz = avroSchema.getProp(field.name());
				try{
				fields.add(Field.createObject(field.name(), Class.forName(clazz)));
				} catch(ClassNotFoundException e){
					throw new PangoolRuntimeException(e);
				}
			case ENUM: //TODO 
			default:
				throw new PangoolRuntimeException("Avro type:" + type + " can't be converted to Pangool Schema type");
			}
		}
		Schema schema = new Schema(avroSchema.getFullName(), fields);
		return schema;
	}

	/**
	 * Converts from one Pangool schema to one Avro schema for serializing it
	 */
	public static org.apache.avro.Schema toAvroSchema(Schema pangoolSchema) {
		List<org.apache.avro.Schema.Field> avroFields = new ArrayList<org.apache.avro.Schema.Field>();
		Map<String, String> complexTypesMetadata = new HashMap<String, String>();
		for(Field field : pangoolSchema.getFields()) {
			org.apache.avro.Schema.Type avroFieldType = null;
			switch(field.getType()){
				case INT: avroFieldType = org.apache.avro.Schema.Type.INT; break;
				case FLOAT: avroFieldType = org.apache.avro.Schema.Type.FLOAT; break;
				case DOUBLE: avroFieldType = org.apache.avro.Schema.Type.DOUBLE; break;
				case LONG: avroFieldType = org.apache.avro.Schema.Type.LONG; break;
				case BOOLEAN: avroFieldType = org.apache.avro.Schema.Type.BOOLEAN; break;
				case STRING: avroFieldType = org.apache.avro.Schema.Type.STRING; break; 
				case OBJECT: 
					avroFieldType = org.apache.avro.Schema.Type.BYTES; 
					complexTypesMetadata.put(field.getName(), field.getObjectClass().getName());
				break;
				case ENUM: //TODO
				default:
					throw new PangoolRuntimeException("Not avro conversion from Pangool Schema type:" + field.getType());
				
			}
			org.apache.avro.Schema avroFieldSchema = org.apache.avro.Schema.create(avroFieldType);
			avroFields.add(new org.apache.avro.Schema.Field(field.getName(),avroFieldSchema, null, null));
		}

		org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord(pangoolSchema.getName(), null, null, false);
		for(Map.Entry<String, String> props : complexTypesMetadata.entrySet()) {
			avroSchema.addProp(props.getKey(), props.getValue());
		}
		avroSchema.setFields(avroFields);
		return avroSchema;
	}

	/**
	 * Moves data between a Tuple and an Avro Record
	 */
	public static void toRecord(Schema pangoolSchema, org.apache.avro.Schema avroSchema, ITuple tuple, Record record,
	    DataOutputBuffer tmpOutputBuffer, HadoopSerialization ser) throws IOException {
		// Convert Tuple to Record
		for(int i = 0; i < pangoolSchema.getFields().size(); i++) {
			Object obj = tuple.get(i);
			if(obj instanceof Text) {
				obj = new Utf8(((Text) obj).toString()).toString();
			}
			Field field = pangoolSchema.getField(i);
			String clazz = avroSchema.getProp(pangoolSchema.getField(i).getName());
			if(clazz != null) {
				tmpOutputBuffer.reset();
				ser.ser(obj, tmpOutputBuffer);
				ByteBuffer buffer = ByteBuffer.wrap(tmpOutputBuffer.getData(), 0, tmpOutputBuffer.getLength());
				record.put(field.getName(), buffer);
			} else {
				record.put(field.getName(), obj);
			}
		}
	}

	/**
	 * Moves data between a Record and a Tuple
	 */
	public static void toTuple(Record record, ITuple tuple, Configuration conf,
	    HadoopSerialization ser) throws ClassNotFoundException, IOException {
		int index = 0;
		for(org.apache.avro.Schema.Field field : record.getSchema().getFields()) {
			Object obj = record.get(field.name());
			if(obj instanceof Utf8) {
				Utf8 utf8 = (Utf8) obj;
				if(tuple.get(index) == null) {
					tuple.set(index, new com.datasalt.pangool.io.Utf8());
				}
				((com.datasalt.pangool.io.Utf8) tuple.get(index)).set(utf8.getBytes(), 0, utf8.getByteLength());
			} else if(obj instanceof ByteBuffer) {
				String clazz = record.getSchema().getProp(field.name());
				if(clazz != null) {
					if(tuple.get(index) == null) {
						tuple.set(index, ReflectionUtils.newInstance(Class.forName(clazz), conf));
					}
					ByteBuffer byteBuffer = (ByteBuffer) obj;
					byte[] bytes = byteBuffer.array();
					ser.deser(tuple.get(index), bytes, 0, bytes.length);
				}
			} else if(obj instanceof Comparable) {
				tuple.set(index, obj);
			}
			index++;
		}
	}
}

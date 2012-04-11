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
import org.apache.hadoop.util.ReflectionUtils;

import com.datasalt.pangool.PangoolRuntimeException;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.serialization.HadoopSerialization;

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
			case LONG: fields.add(Field.create(field.name(),Type.LONG)); break;
			case FLOAT: fields.add(Field.create(field.name(),Type.FLOAT)); break;
			case DOUBLE: fields.add(Field.create(field.name(),Type.DOUBLE)); break;
			case BOOLEAN: fields.add(Field.create(field.name(),Type.BOOLEAN)); break;
			case STRING: fields.add(Field.create(field.name(),Type.STRING)); break;
			case BYTES:
				String clazz = avroSchema.getProp(field.name());
				if (clazz != null){
					try{
						fields.add(Field.createObject(field.name(), Class.forName(clazz)));
						} catch(ClassNotFoundException e){
							throw new PangoolRuntimeException(e);
						}
				} else {
					fields.add(Field.create(field.name(),Type.BYTES));
				}
				break;
			case ENUM:
				clazz = avroSchema.getProp(field.name());
				try{
					fields.add(Field.createEnum(field.name(),Class.forName(clazz)));
					} catch(ClassNotFoundException e){
						throw new PangoolRuntimeException(e);
					}
				break;
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
				case BYTES: avroFieldType = org.apache.avro.Schema.Type.BYTES; break;
				case OBJECT: 
					avroFieldType = org.apache.avro.Schema.Type.BYTES; 
					complexTypesMetadata.put(field.getName(), field.getObjectClass().getName());
				break;
				case ENUM: 
					avroFieldType = org.apache.avro.Schema.Type.ENUM;
					complexTypesMetadata.put(field.getName(),field.getObjectClass().getName());
					break;
				default:
					throw new PangoolRuntimeException("Not avro conversion from Pangool Schema type:" + field.getType());
				
			}
			org.apache.avro.Schema avroFieldSchema;
			if (avroFieldType == org.apache.avro.Schema.Type.ENUM){
				Object[] enumValues = field.getObjectClass().getEnumConstants();
				List<String> values=new ArrayList<String>();
				for (Object e : enumValues){
					values.add(e.toString());
				}
				avroFieldSchema = org.apache.avro.Schema.createEnum(field.getName(),null,null, values);
			} else {
				//simple types
				avroFieldSchema = org.apache.avro.Schema.create(avroFieldType);
			}
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
	public static void toRecord(ITuple tuple, Record record,
			DataOutputBuffer tmpOutputBuffer, HadoopSerialization ser) throws IOException {
		Schema pangoolSchema = tuple.getSchema();
		for(int i = 0; i < pangoolSchema.getFields().size(); i++) {
			Object obj = tuple.get(i);
			Field field = pangoolSchema.getField(i);
			switch(field.getType()){
			case INT:
			case LONG:
			case FLOAT:
			case BOOLEAN:
			case DOUBLE:
			case BYTES:
				record.put(i, obj); //optimistic
				break;
			case OBJECT:
				tmpOutputBuffer.reset();
				ser.ser(obj, tmpOutputBuffer);
				ByteBuffer buffer = ByteBuffer.wrap(tmpOutputBuffer.getData(), 0, tmpOutputBuffer.getLength());
				record.put(i, buffer);
				break;
			case ENUM:
				record.put(i,obj.toString());
				break;
			case STRING:
				record.put(i,new Utf8(obj.toString())); //could be directly String ?
				break;
			default:
					throw 
					new IOException("Not correspondence to Avro type from Pangool type " + field.getType());
			}
		}
	}

	/**
	 * Moves data between a Record and a Tuple
	 */
	public static void toTuple(Record record, ITuple tuple, Configuration conf,
	    HadoopSerialization ser) throws IOException {
		for(org.apache.avro.Schema.Field field : record.getSchema().getFields()) {
			int pos = field.pos();
			Object obj = record.get(pos);
			switch(field.schema().getType()){
			case INT:
			case LONG:
			case BOOLEAN:
			case FLOAT:
			case DOUBLE:
				tuple.set(pos,obj); //very optimistic
				break;
			case STRING:
				if (!(tuple.get(pos) instanceof Utf8)){
					tuple.set(pos,new com.datasalt.pangool.io.Utf8());
				}
				com.datasalt.pangool.io.Utf8 utf8=(com.datasalt.pangool.io.Utf8)tuple.get(pos);
				if (obj instanceof String){
					utf8.set((String)obj);
				} else if (obj instanceof Utf8){
					Utf8 avroUtf8 = (Utf8)obj;
					utf8.set(avroUtf8.getBytes(),0,avroUtf8.getByteLength());
				} else {
					throw new IOException("Not supported avro field " + 
							org.apache.avro.Schema.Type.STRING + " with instance " + obj.getClass().getName());
				}
				break;
			case ENUM:
				String clazzName = record.getSchema().getProp(field.name());
				try{
					Class clazz = Class.forName(clazzName);
					Enum e = Enum.valueOf(clazz,obj.toString());
					tuple.set(pos,e);
				} catch(ClassNotFoundException e){
					throw new IOException(e);
				}
				break;
			case BYTES:
				clazzName = record.getSchema().getProp(field.name());
				if (clazzName != null){
				try{
					Class clazz = Class.forName(clazzName);
					if(tuple.get(pos) == null || tuple.get(pos).getClass() != clazz) {
						tuple.set(pos, ReflectionUtils.newInstance(clazz, conf));
					}
					ByteBuffer byteBuffer = (ByteBuffer) obj;
					byte[] bytes = byteBuffer.array();
					ser.deser(tuple.get(pos), bytes, 0, bytes.length);
				} catch(ClassNotFoundException e){
					throw new IOException(e);
				}} else {
					tuple.set(pos, obj); //this should be byte[] or ByteBuffer
				}
				break;
			default:
				throw new IOException("Not supported avro type : " + field.schema().getType());
			}
		}
	}
}

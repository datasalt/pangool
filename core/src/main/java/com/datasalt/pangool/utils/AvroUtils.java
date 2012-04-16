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
import com.datasalt.pangool.io.Schema.Field.FieldDeserializer;
import com.datasalt.pangool.io.Schema.Field.FieldSerializer;
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
		for(org.apache.avro.Schema.Field avroField : avroSchema.getFields()) {
			org.apache.avro.Schema.Type type = avroField.schema().getType();
			Field pangoolField;
			switch(type){
			case INT: pangoolField = Field.create(avroField.name(),Type.INT); break;
			case LONG: pangoolField = Field.create(avroField.name(),Type.LONG); break;
			case FLOAT: pangoolField = Field.create(avroField.name(),Type.FLOAT); break;
			case DOUBLE: pangoolField = Field.create(avroField.name(),Type.DOUBLE); break;
			case BOOLEAN: pangoolField = Field.create(avroField.name(),Type.BOOLEAN); break;
			case STRING: pangoolField = Field.create(avroField.name(),Type.STRING); break;
			case BYTES:
				
				String objectClazz = avroField.getProp(Field.METADATA_OBJECT_CLASS);
				if (objectClazz != null){
					try {
					String serializerString = avroField.getProp(Field.METADATA_OBJECT_SERIALIZER);
					String deserializerString = avroField.getProp(Field.METADATA_OBJECT_DESERIALIZER);
					Class<? extends FieldSerializer> ser;
          
	          ser = (serializerString == null) ? null : 
	          	(Class<? extends FieldSerializer>)Class.forName(serializerString);
          
					Class<? extends FieldDeserializer> deser = (deserializerString == null) ? null:
						(Class<? extends FieldDeserializer>)Class.forName(deserializerString);
					pangoolField = Field.createObject(avroField.name(), Class.forName(objectClazz),
								ser,deser);
          } catch(ClassNotFoundException e) {
	          throw new PangoolRuntimeException(e);
          }
				} else {
					//if no objectClazz present then it's just BYTES
					pangoolField = Field.create(avroField.name(),Type.BYTES);
				}
				break;
			case ENUM:
				objectClazz = avroField.getProp(Field.METADATA_OBJECT_CLASS);
				try{
					pangoolField = Field.createEnum(avroField.name(),Class.forName(objectClazz));
					} catch(ClassNotFoundException e){
						throw new PangoolRuntimeException(e);
					}
				break;
			default:
				throw new PangoolRuntimeException("Avro type:" + type + " can't be converted to Pangool Schema type");
			}
			//add properties 
			for(Map.Entry<String,String> entry : avroField.props().entrySet()){
				if (!Field.RESERVED_KEYWORDS.contains(entry.getKey())){
					pangoolField.addProp(entry.getKey(),entry.getValue());
				}
			}
			fields.add(pangoolField);
			
		}
		Schema schema = new Schema(avroSchema.getFullName(), fields);
		return schema;
	}

	
	
	/**
	 * Converts from one Pangool schema to one Avro schema for serializing it
	 */
  public static org.apache.avro.Schema toAvroSchema(Schema pangoolSchema) {
		List<org.apache.avro.Schema.Field> avroFields = new ArrayList<org.apache.avro.Schema.Field>();

		for(Field pangoolField : pangoolSchema.getFields()) {
			org.apache.avro.Schema.Type avroFieldType = null;
			org.apache.avro.Schema.Field avroField = null;
			org.apache.avro.Schema avroFieldSchema=null;
			switch(pangoolField.getType()){
				case INT: 
				case FLOAT: 
				case DOUBLE: 
				case LONG: 
				case BOOLEAN: 
				case STRING:  
				case BYTES:
					avroFieldType = org.apache.avro.Schema.Type.valueOf(pangoolField.getType().toString());
					avroFieldSchema =org.apache.avro.Schema.create(avroFieldType); 
					avroField = new org.apache.avro.Schema.Field(pangoolField.getName(),avroFieldSchema
							,null,null);
				break;
				case OBJECT: 
					avroFieldType = org.apache.avro.Schema.Type.BYTES; 
					avroFieldSchema = org.apache.avro.Schema.create(avroFieldType);
					avroField = new org.apache.avro.Schema.Field(pangoolField.getName(),avroFieldSchema
							,null,null);
					
				break;
				case ENUM: 
					avroFieldType = org.apache.avro.Schema.Type.ENUM;
					Object[] enumValues = pangoolField.getObjectClass().getEnumConstants();
					List<String> values=new ArrayList<String>();
					for (Object e : enumValues){
						values.add(e.toString());
					}
					avroFieldSchema = org.apache.avro.Schema.createEnum(pangoolField.getName(),null,null, values);
					avroField = new org.apache.avro.Schema.Field(pangoolField.getName(),avroFieldSchema
							,null,null);
					break;
				default:
					throw new PangoolRuntimeException("Not avro conversion from Pangool Schema type:" + pangoolField.getType());
				
			}
			if (pangoolField.getObjectClass() != null){
				avroField.addProp(Field.METADATA_OBJECT_CLASS, pangoolField.getObjectClass().getName());
			}
			if (pangoolField.getSerializerClass() != null){
				avroField.addProp(Field.METADATA_OBJECT_SERIALIZER,pangoolField.getSerializerClass().getName());
			}
			if (pangoolField.getSerializerClass() != null){
				avroField.addProp(Field.METADATA_OBJECT_DESERIALIZER,pangoolField.getDeserializerClass().getName());
			}
			for (Map.Entry<String,String> property : pangoolField.getProps().entrySet()){
				avroField.addProp(property.getKey(),property.getValue());
			}
			avroFields.add(avroField);
		}
		org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord(pangoolSchema.getName(), null, null, false);
		avroSchema.setFields(avroFields);
		return avroSchema;
	}

	

	
	
}

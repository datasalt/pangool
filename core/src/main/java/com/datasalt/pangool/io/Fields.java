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
package com.datasalt.pangool.io;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.tuplemr.serialization.AvroFieldSerialization;
import com.datasalt.pangool.tuplemr.serialization.TupleFieldSerialization;

public class Fields {

	private final static Map<String, Type> strToType = new HashMap<String, Type>();

	static {
		strToType.put("int", Type.INT);
		strToType.put("long", Type.LONG);
		strToType.put("boolean", Type.BOOLEAN);
		strToType.put("float", Type.FLOAT);
		strToType.put("double", Type.DOUBLE);
		
		strToType.put("string", Type.STRING);
		strToType.put("utf8",Type.STRING);
	}

	public static List<Field> parse(String serialized) {
		if(serialized == null || serialized.isEmpty()) {
			return null;
		}
		String[] fieldsStr = serialized.split(",");
		List<Field> fields = new ArrayList<Field>();
		for(String field : fieldsStr) {
			String[] nameType = field.split(":");
			if(nameType.length != 2) {
				throw new RuntimeException("Incorrect fields description " + serialized);
			}
			String fieldName = nameType[0].trim();
			String fieldType = nameType[1].trim();
			Type type = strToType.get(fieldType);
			try {
				if(type != null) {
					fields.add(Field.create(fieldName, type));
				} else {
					Class<?> objectClazz = Class.forName(fieldType);
					if(objectClazz.isEnum()) {
						fields.add(Field.createEnum(fieldName, objectClazz));
					} else {
						fields.add(Field.createObject(fieldName, objectClazz));
					}
				}
			} catch(ClassNotFoundException e) {
				throw new RuntimeException("Type " + fieldType
				    + " not a valid class name ", e);
			}
		}
		return fields;
	}
	
	/**
	 * @see {@link #createAvroField(String, org.apache.avro.Schema, boolean)}
	 */
	public static Field createAvroField(String name,org.apache.avro.Schema avroSchema){
		return createAvroField(name,avroSchema,false);
	}
	
	/**
	 * Creates a field containing an Avro object that will be serialized using 
	 * {@link AvroFieldSerialization}
	 * 
	 * @param name Field's name
	 * @param avroSchema The schema of the field
	 * @param isReflect If the object to be serialized needs reflection to be serialized
	 * or deserialized
	 * @return
	 */
	public static Field createAvroField(String name,
			 org.apache.avro.Schema avroSchema,	boolean isReflect){
		Field field = Field.createObject(name,Object.class);
		field.setObjectSerialization(AvroFieldSerialization.class);
		field.addProp("avro.schema",avroSchema.toString());
		field.addProp("avro.reflection",Boolean.toString(isReflect));
		return field;
	}
	
	/**
	 * Creates a field containing a Pangool Tuple.
	 * 
	 * @param name Field's name
	 * @param schema The schema of the field
	 * @return
	 */
	public static Field createTupleField(String name, Schema schema) {
		Field field = Field.createObject(name,Object.class);
		field.setObjectSerialization(TupleFieldSerialization.class);
		field.addProp("schema", schema.toString());
		return field;
	}
}

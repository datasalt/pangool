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
package com.datasalt.pangool.io.tuple;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import com.datasalt.pangool.PangoolRuntimeException;
import com.datasalt.pangool.io.Utf8;

/**
 * Encapsulates one Pangool schame composed of {@link Field} instances.
 */
public class Schema {

	static final JsonFactory FACTORY = new JsonFactory();
	static final ObjectMapper MAPPER = new ObjectMapper(FACTORY);

	static {
		FACTORY.enable(JsonParser.Feature.ALLOW_COMMENTS);
		FACTORY.setCodec(MAPPER);
	}

	public static enum InternalType {
		INT("int", Integer.class), 
		VINT("vint", VIntWritable.class), 
		LONG("long", Long.class), 
		VLONG("vlong", VLongWritable.class), 
		FLOAT("float", Float.class), 
		DOUBLE("double", Double.class), 
		UTF8("utf8", Utf8.class), 
		BOOLEAN("boolean", Boolean.class), 
		ENUM(null, null), 
		OBJECT(null, null);

		private String parsingString;
		private Class<?> representativeClass;
		
		private static HashMap<Class<?>, InternalType> fromClass; 
		static {
			fromClass = new HashMap<Class<?>, InternalType>();
			for (InternalType iType : InternalType.values()) {
				if (iType.getRepresentativeClass() != null) {
					fromClass.put(iType.getRepresentativeClass(), iType);
				}
			}
		}

		InternalType(String parsingString, Class<?> representativeClass) {
			this.parsingString = parsingString;
			this.representativeClass = representativeClass;
		}

		/**
		 * Returns the string that represents this type. For example "int" 
		 * for Integer, etc. Used at {@link Fields#parse(String)} method.
		 * Not present for all {@link InternalType}
		 */
		public String getParsingString() {
			return parsingString;
		}
		
		/**
		 * Returns the representative class for this InternalType. 
		 * For example {@link #INT} is represented by {@link Integer}
		 * and {@link #VINT} is represented by {@link VIntWritable}
		 */
		public Class<?> getRepresentativeClass() {
			return representativeClass;
		}
		
		/**
		 * Maps from class to InternalType
		 */
		public static InternalType fromClass(Class<?> clazz) {
			InternalType iType = fromClass.get(clazz);
			if (iType != null) {
				return iType;
			} else if (clazz.isEnum()) {
				return ENUM;
			} else {
				return OBJECT;
			}
		}
	}

	public static class Field {

		private String name;
		private Class<?> type;
		private InternalType iType;

		public Field(String name, Class<?> clazz) {
			if(name == null) {
				throw new IllegalArgumentException("Field name can't be null");
			}

			if(clazz == null) {
				throw new IllegalArgumentException("Field type can't be null");
			}
			this.name = name;
			setType(clazz);
		}
		
		private void setType(Class<?> type) {
			this.type = type;
			this.iType = InternalType.fromClass(type);			
		}

		public Class<?> getType() {
			return type;
		}

		public String getName() {
			return name;
		}
		
		public InternalType getInternalType() {
			return iType;
		}

		public boolean equals(Object a) {
			if(!(a instanceof Field)) {
				return false;
			}
			Field that = (Field) a;
			return name.equals(that.getName()) && type.equals(that.getType());
		}

		public String toString() {
			try {
				StringWriter writer = new StringWriter();
				JsonGenerator gen = FACTORY.createJsonGenerator(writer);
				toJson(gen);
				gen.flush();
				return writer.toString();
			} catch(IOException e) {
				throw new RuntimeException(e);
			}
		}

		static Field parse(JsonNode node) throws IOException {
			String name = node.get("name").getTextValue();
			String clazz = node.get("type").getTextValue();
			try {
				return new Field(name, Class.forName(clazz));
			} catch(ClassNotFoundException e) {
				throw new IOException(e);
			}
		}

		void toJson(JsonGenerator gen) throws IOException {
			gen.writeStartObject();
			gen.writeStringField("name", getName());
			gen.writeStringField("type", getType().getName());
			gen.writeEndObject();
		}
	}

	private final List<Field> fields;
	private final String name;

	private Map<String, Integer> indexByFieldName = new HashMap<String, Integer>();

	public Schema(String name, List<Field> fields) {
		if(name == null || name.isEmpty()) {
			throw new IllegalArgumentException("Name for schema can't be null");
		}
		this.name = name;
		this.fields = Collections.unmodifiableList(new ArrayList<Field>(fields));

		int index = 0;
		for(Field field : this.fields) {
			this.indexByFieldName.put(field.getName(), index);
			index++;
		}
	}

	public List<Field> getFields() {
		return fields;
	}

	public String getName() {
		return name;
	}

	public Integer getFieldPos(String fieldName) {
		return indexByFieldName.get(fieldName);
	}

	public Field getField(String fieldName) {
		Integer index = getFieldPos(fieldName);
		return (index == null) ? null : fields.get(index);
	}

	public Field getField(int i) {
		return fields.get(i);
	}

	public boolean containsField(String fieldName) {
		return indexByFieldName.containsKey(fieldName);
	}

	@Override
	public String toString() {
		return toString(true);
	}

	public String toString(boolean pretty) {
		try {
			StringWriter writer = new StringWriter();
			JsonGenerator gen = FACTORY.createJsonGenerator(writer);
			if(pretty)
				gen.useDefaultPrettyPrinter();
			toJson(gen);
			gen.flush();
			return writer.toString();
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
	}

	
	public static Schema parse(String s) {
		try {
			return parse(FACTORY.createJsonParser(new StringReader(s)));
		} catch(IOException e) {
			throw new SchemaParseException(e);
		} 
	}

	
	static Schema parse(JsonParser parser) throws IOException {
			return Schema.parse(MAPPER.readTree(parser));
	}

	public static Schema parse(JsonNode schema) throws IOException {
		String name = schema.get("name").getTextValue();
		List<Field> fields = new ArrayList<Field>();
		JsonNode fieldsNode = schema.get("fields");
		Iterator<JsonNode> fieldsNodes = fieldsNode.getElements();
		while(fieldsNodes.hasNext()) {
			JsonNode fieldNode = fieldsNodes.next();
			fields.add(Field.parse(fieldNode));
		}
		return new Schema(name, fields);
	}

	@Override
	public boolean equals(Object o) {
		if(o instanceof Schema) {
			return toString().equals(o.toString());
		}
		return false;
	}

	public void toJson(JsonGenerator gen) throws IOException {
		gen.writeStartObject();
		gen.writeStringField("name", name);
		gen.writeFieldName("fields");
		fieldsToJson(gen);
		gen.writeEndObject();
	}

	private void fieldsToJson(JsonGenerator gen) throws IOException {
		gen.writeStartArray();
		for(Field f : fields) {
			f.toJson(gen);
		}
		gen.writeEndArray();
	}
	
	@SuppressWarnings("serial")
  public static class SchemaParseException extends PangoolRuntimeException {
	  public SchemaParseException(Throwable cause) { super(cause); }
	  public SchemaParseException(String message) { super(message); }
	}
	
}

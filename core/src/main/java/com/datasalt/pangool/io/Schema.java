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

import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import com.datasalt.pangool.PangoolRuntimeException;

/**
 * A list of {@link Field} elements that a {@link ITuple} instance contains.
 * 
 */
public class Schema implements Serializable{

	static final JsonFactory FACTORY = new JsonFactory();
	static final ObjectMapper MAPPER = new ObjectMapper(FACTORY);

	static {
		FACTORY.enable(JsonParser.Feature.ALLOW_COMMENTS);
		FACTORY.setCodec(MAPPER);
	}

	/**
	 * A field is an abstract data type that can be one of this:
	 * <ul>
	 * <li>A 32-bit signed <i>int</i>;
	 * <li>A 64-bit signed <i>long</i>;
	 * <li>A 32-bit IEEE single-<i>float</i>; or
	 * <li>A 64-bit IEEE <i>double</i>-float; or
	 * <li>A unicode <i>string</i>;
	 * <li>A <i>boolean</i>; or
	 * <li>An <i>enum</i>, containing one of a small set of symbols;
	 * <li>An arbitrary <i>object</i>, serializable by Hadoop's serialization
	 * </ul>
	 * 
	 * A field can be constructed using one of its static <tt>createXXX</tt>
	 * methods. A field object is <b>immutable</b>.
	 */
	public static class Field implements Serializable{
		public static enum Type {
			INT, LONG, FLOAT, DOUBLE, STRING, BOOLEAN, ENUM, OBJECT;
		}

		private final String name;
		private final Type type;
		private final Class<?> objectClass;

		public static Field create(String name, Type type) {
			if(type == Type.ENUM) {
				throw new IllegalArgumentException(
				    "Not allowed 'ENUM' type. Use 'Field.createEnum' method");
			} else if(type == Type.OBJECT) {
				throw new IllegalArgumentException(
				    "Not allowed 'OBJECT' type. Use 'Field.createObject' method");
			}
			return new Field(name, type, null);
		}

		/**
		 * Creates an <i>object</i> field.
		 * 
		 * @param name
		 *          Field's name
		 * @param clazz
		 *          Object's instance class
		 * @return
		 */
		public static Field createObject(String name, Class<?> clazz) {
			return new Field(name, Type.OBJECT, clazz);
		}

		/**
		 * Creates an enum field, based in a enum class
		 * 
		 * @param name
		 *          Field's name
		 * @param clazz
		 *          Enum class
		 * @return
		 */
		public static Field createEnum(String name, Class<?> clazz) {
			return new Field(name, Type.ENUM, clazz);
		}

		private Field(String name, Type type, Class<?> clazz) {
			if(name == null) {
				throw new IllegalArgumentException("Field name can't be null");
			}
			if(type == null) {
				throw new IllegalArgumentException("Field type can't be null");
			} else if(type == Type.OBJECT || type == Type.ENUM) {
				if(clazz == null) {
					throw new IllegalArgumentException("Field with type " + type
					    + " must specify object class");
				}

				if(type == Type.ENUM && !clazz.isEnum()) {
					throw new IllegalArgumentException("Field with type " + type
					    + " must specify an enum class.Use createEnum.");
				}
				this.objectClass = clazz;
			} else {
				this.objectClass = null;
			}
			this.name = name;
			this.type = type;

		}

		public Type getType() {
			return type;
		}

		public String getName() {
			return name;
		}

		public Class<?> getObjectClass() {
			return objectClass;
		}

		public boolean equals(Object a) {
			if(!(a instanceof Field)) {
				return false;
			}
			Field that = (Field) a;

			boolean t = name.equals(that.getName()) && type.equals(that.getType());
			if(type == Type.OBJECT || type == Type.ENUM) {
				return t && objectClass.equals(that.getObjectClass());
			} else {
				return t;
			}
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
			try {
				String name = node.get("name").getTextValue();
				String typeStr = node.get("type").getTextValue();
				Type type = Type.valueOf(typeStr);
				switch(type) {
				case OBJECT:
					String clazz = node.get("object_class").getTextValue();
					return Field.createObject(name, Class.forName(clazz));
				case ENUM:
					clazz = node.get("object_class").getTextValue();
					return Field.createEnum(name, Class.forName(clazz));
				default:
					return Field.create(name, type);
				}
			} catch(ClassNotFoundException e) {
				throw new IOException(e);
			}
		}

		void toJson(JsonGenerator gen) throws IOException {
			gen.writeStartObject();
			gen.writeStringField("name", getName());
			gen.writeStringField("type", getType().toString());
			if(getType() == Type.OBJECT || getType() == Type.ENUM) {
				gen.writeStringField("object_class", getObjectClass().getName());
			}
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
		public SchemaParseException(Throwable cause) {
			super(cause);
		}

		public SchemaParseException(String message) {
			super(message);
		}
	}

}

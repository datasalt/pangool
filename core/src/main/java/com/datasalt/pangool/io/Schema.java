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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.AvroRuntimeException;
import org.apache.hadoop.io.serializer.Serialization;
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
@SuppressWarnings("serial")
public class Schema implements Serializable {

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
	 * <li>A <i>byte buffer</i>
	 * <li>An arbitrary <i>object</i>, serializable by Hadoop's serialization
	 * </ul>
	 * 
	 * A field can be constructed using one of its static <tt>createXXX</tt>
	 * methods. A field object is <b>immutable</b>.
	 */
	public static class Field implements Serializable{
		/**
		 *	Interface that allows to pass field's metadata to the object that implements it.
		 *  Used to permit stateful custom serialization for fields. 
		 */
		public static interface FieldConfigurable {
		  /**
		   * Sets the properties for that field
			 *
		   * @param props Properties of that field
		   */
			public void setFieldProperties(Map<String,String> props);
		}

		public static enum Type {
			INT, LONG, FLOAT, DOUBLE, STRING, BOOLEAN, ENUM, BYTES,OBJECT;
		}
		
		public static final Set<String> RESERVED_KEYWORDS;
		public static final String METADATA_OBJECT_CLASS="pangool.object.java-class";
		public static final String METADATA_OBJECT_SERIALIZATION="pangool.object.java-serialization-class";
		//this is to mark a BYTES Avro type as pangool OBJECT
		public static final String METADATA_BYTES_AS_OBJECT="pangool.object.mark";
		
		static {
			Set <String> reserved = new HashSet<String>();
			Collections.addAll(reserved, 
					METADATA_OBJECT_CLASS,METADATA_OBJECT_SERIALIZATION,
					METADATA_BYTES_AS_OBJECT);
			RESERVED_KEYWORDS = Collections.unmodifiableSet(reserved);
		}
		
		static final class Props extends LinkedHashMap<String,String> {
	    private Set<String> reserved;
	    public Props(Set<String> reserved) {
	      super(1);
	      this.reserved = reserved;
	    }
	    public void add(String name, String value) {
	      if (reserved.contains(name))
	        throw new AvroRuntimeException("Can't set reserved property: " + name);
	      
	      if (value == null)
	        throw new AvroRuntimeException("Can't set a property to null: " + name);
	    
	      String old = get(name);
	      if (old == null)
	        put(name, value);
	      else if (!old.equals(value))
	        throw new AvroRuntimeException("Can't overwrite property: " + name);
	    }

	    public void write(JsonGenerator gen) throws IOException {
	      for (Map.Entry<String,String> e : entrySet())
	        gen.writeStringField(e.getKey(), e.getValue());
	    }
	  }
		

		private final String name;
		private final Type type;
		
		private final Props props= new Props(RESERVED_KEYWORDS);
		
		//special properties in props 
		private Class<?> objectClass; //lazy loaded
		private Class<? extends Serialization> serializationClass;
		
		public void addProp(String key,String value){
			props.add(key, value);
		}
		
		public Map<String,String> getProps(){
			return Collections.unmodifiableMap(props);
		}
		
		public String getProp(String name){
			return props.get(name);
		}
		
		
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
		
		
		public static Field cloneField(Field field, String newName){
			Field result;
			switch(field.getType()){
			case OBJECT:
					result = Field.createObject(newName,field.getObjectClass());
					result.setObjectSerialization(field.getObjectSerialization());
				 break;
			case ENUM:
				result =  Field.createEnum(newName,field.getObjectClass());
				break;
				default:
					result= Field.create(newName,field.getType());
			}
			
			for(Map.Entry<String,String> entry : field.getProps().entrySet()){
				if (!RESERVED_KEYWORDS.contains(entry.getKey())){
					result.addProp(entry.getKey(),entry.getValue());
				}
			}
			return result;
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
			} 
			
			switch(type){
			case OBJECT:
				if (clazz == null){
					throw new IllegalArgumentException("Field needs specify object class");
				}
				break;
			case ENUM:
				if(clazz == null) {
					throw new IllegalArgumentException("Enum field must specify enum class");
				}
				if(!clazz.isEnum()) {
					throw new IllegalArgumentException("Field with type " + type
					    + " must specify an enum class.Use createEnum.");
				}
				break;
			}
			this.objectClass = clazz;
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
		
		public Class<? extends Serialization> getObjectSerialization(){
			return serializationClass;
		}
		
		/**
		 * Sets custom serialization for fields with type OBJECT.
		 * If the Serialization class also implements {@link FieldConfigurable} then 
		 * the field's metadata (properties) is passed to the instance allowing stateful serialization. 
		 */
		public void setObjectSerialization(Class<? extends Serialization> serialization){
			if (type != Type.OBJECT){
				throw new PangoolRuntimeException("Can't set custom serialization for type " + type);
			}
			if (serializationClass != null){
				throw new PangoolRuntimeException("Serialization already set :" + serializationClass);
			}
			serializationClass = serialization;
		}
		
		public boolean equals(Object a) {
			if(!(a instanceof Field)) {
				return false;
			}
			Field that = (Field) a;

			boolean t = name.equals(that.getName()) && type.equals(that.getType());
					
			if(type == Type.OBJECT || type == Type.ENUM) {
				t = t && objectClass.equals(that.getObjectClass());
			} 
			
			if (serializationClass == null && that.serializationClass != null || serializationClass != null && that.serializationClass == null){
				return false;
			} else if (serializationClass != null && that.serializationClass != null){
				t = t && serializationClass.equals(that.serializationClass);
			}
			
			if (props == null && that.props != null || props != null && that.props == null){
				return false;
			} else if (props != null && that.props != null){
				t = t && props.equals(that.props); 
			}
			return t;
		}			

		@Override
		public int hashCode() {
			return name.hashCode(); //FIXME bad hash code
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
				
				Map<String,String> properties =null;
				Field field;
				switch(type) {
				case OBJECT:{
					JsonNode clazzNode = node.get("object_class");
					String clazz = clazzNode.getTextValue();
					field = Field.createObject(name,Class.forName(clazz));
					if (node.get("serialization") != null){
						Class ser = Class.forName(node.get("serialization").getTextValue());
						field.setObjectSerialization(ser);
					}
					break;
				}
				case ENUM:{
					String clazz = node.get("object_class").getTextValue();
					field =  Field.createEnum(name, Class.forName(clazz));
					break;
				}
				default:
					field =  Field.create(name, type);
				}
				if (node.get("properties") != null){
					JsonNode propNode = node.get("properties");
					Iterator<String> fieldNames = propNode.getFieldNames();
					while (fieldNames.hasNext()){
						String key = fieldNames.next();
						field.addProp(key,propNode.get(key).getTextValue());
					}
				}
				return field;
			} catch(ClassNotFoundException e) {
				throw new IOException(e);
			}
		}
		
		void toJson(JsonGenerator gen) throws IOException {
			gen.writeStartObject();
			gen.writeStringField("name", getName());
			gen.writeStringField("type", getType().toString());
			if(getType() == Type.ENUM) {
				gen.writeStringField("object_class", getObjectClass().getName());
			} else if (getType() == Type.OBJECT){
				gen.writeStringField("object_class", getObjectClass().getName());
				if(serializationClass != null) {
					gen.writeStringField("serialization",serializationClass.getName());
				}
			}
			if (props != null && !props.isEmpty()){
				gen.writeObjectFieldStart("properties");
				for(Map.Entry<String,String> entry : props.entrySet()){
					gen.writeStringField(entry.getKey(), entry.getValue());
				}
				gen.writeEndObject();
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

	@Override
	public int hashCode() {
		return toString().hashCode();
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

	public static class SchemaParseException extends PangoolRuntimeException {
		public SchemaParseException(Throwable cause) {
			super(cause);
		}

		public SchemaParseException(String message) {
			super(message);
		}
	}
	
	/*
	 * Methods to use field aliases
	 *
	 * TODO : this could be in a utils class
	 */ 
	public static boolean containsFieldUsingAlias(Schema schema,String fieldName,Map<String,String> aliases){
		if (aliases == null){
			return schema.containsField(fieldName);
		} else {
		String ref = aliases.get(fieldName);
		return ref == null ? schema.containsField(fieldName) : schema.containsField(ref);
		}
	}
	
	public static Field getFieldUsingAliases(Schema schema,String field,Map<String,String> aliases){
		if (aliases == null){
			return schema.getField(field);
		} else {
			String ref = aliases.get(field);
			return ref == null ? schema.getField(field) : schema.getField(ref);
		}
	}
	
	public static int getFieldPosUsingAliases(Schema schema,String field,Map<String,String> aliases){
		if (aliases == null){
			return schema.getFieldPos(field);
		} else {
			String ref = aliases.get(field);
			return ref == null ? schema.getFieldPos(field) : schema.getFieldPos(ref);
		}
	}

}

package com.datasalt.pangool;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.apache.avro.SchemaParseException;
import org.apache.commons.collections.BidiMap;
import org.apache.commons.collections.bidimap.DualHashBidiMap;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
/**
 * Encapsulates one Pangool schame composed of {@link Field} instances.
 */
public class Schema {

	static final JsonFactory FACTORY = new JsonFactory();
  static final ObjectMapper MAPPER = new ObjectMapper(FACTORY);

  //private static final int NO_HASHCODE = Integer.MIN_VALUE;

  static {
    FACTORY.enable(JsonParser.Feature.ALLOW_COMMENTS);
    FACTORY.setCodec(MAPPER);
  }
	
	
	public static class PrimitiveTypes {

		public final static String INT = "int";
		public final static String VINT = "vint";
		public final static String LONG = "long";
		public final static String VLONG = "vlong";
		public final static String FLOAT = "float";
		public final static String DOUBLE = "double";
		public final static String STRING = "string";
		public final static String BOOLEAN = "boolean";
	}

	private static final BidiMap strClassMap = new DualHashBidiMap();

	static {
		strClassMap.put(PrimitiveTypes.INT, Integer.class);
		strClassMap.put(PrimitiveTypes.VINT, VIntWritable.class);
		strClassMap.put(PrimitiveTypes.LONG, Long.class);
		strClassMap.put(PrimitiveTypes.VLONG, VLongWritable.class);
		strClassMap.put(PrimitiveTypes.FLOAT, Float.class);
		strClassMap.put(PrimitiveTypes.DOUBLE, Double.class);
		strClassMap.put(PrimitiveTypes.STRING, String.class);
		strClassMap.put(PrimitiveTypes.BOOLEAN, Boolean.class);
	}
	
	public static class Field {
		
		private String name;
		private Class<?> type;

		public Field() {
		}

		public Field(String name, Class<?> clazz) {
			this.name = name;
			this.type = clazz;
		}

		public Class<?> getType() {
			return type;
		}

		public String name() {
			return name;
		}
		
		public String toString() {
			return name + ":" + type;
		}
		
		static Field parse(JsonNode node) throws IOException {
		 String name = node.get("name").getTextValue();
		 String clazz = node.get("type").getTextValue();
		 try{
		 return new Field(name,strToClass(clazz));
		 } catch(ClassNotFoundException e){
			 throw new IOException(e);
		 }
		}
		
		void toJson(JsonGenerator gen) throws IOException {
			gen.writeStartObject();
      gen.writeStringField("name", name());
      gen.writeStringField("type",getType().getName());
      gen.writeEndObject();
		}
		
		
	}

	private final List<Field> fields;
	private final String name;

	public static Class<?> strToClass(String str) throws ClassNotFoundException {
		Class<?> clazz = (Class<?>) strClassMap.get(str);
		if(clazz == null) {
			clazz = Class.forName(str);
		}
		return clazz;
	}

	public static String classToStr(Class<?> clazz) {
		String clazzStr = (String) strClassMap.inverseBidiMap().get(clazz);
		if(clazzStr == null) {
			return clazz.getName().toString();
		} else {
			return clazzStr;
		}
	}

	private Map<String, Integer> indexByFieldName = new HashMap<String, Integer>();

	public Schema(String name,List<Field> fields) {
		if (name == null || name.isEmpty()){
			throw new IllegalArgumentException("Name for schema can't be null");
		}
		this.name = name;
		this.fields = Collections.unmodifiableList(new ArrayList<Field>(fields));
		
		int index = 0;
		for(Field field : this.fields) {
			this.indexByFieldName.put(field.name(), index);
			index++;
		}
	}

	public List<Field> getFields() {
		return fields;
	}
	
	public String getName(){
		return name;
	}

	public Integer getFieldPos(String fieldName){
		return indexByFieldName.get(fieldName);
	}
	
	public Field getField(String fieldName) {
		Integer index = getFieldPos(fieldName);
		return (index == null) ? null : fields.get(index);
	}

	public Field getField(int i) {
		return fields.get(i);
	}

	public String serialize() {
		StringBuilder b = new StringBuilder();
		b.append(name).append(";");
		String fieldName = fields.get(0).name;
		Class<?> fieldType = fields.get(0).type;
		String clazzStr = classToStr(fieldType);
		if(clazzStr == null) {
			clazzStr = fieldType.getName();
		}
		b.append(fieldName).append(":").append(clazzStr);
		for(int i = 1; i < fields.size(); i++) {
			fieldName = fields.get(i).name;
			fieldType = fields.get(i).type;
			clazzStr = classToStr(fieldType);
			if(clazzStr == null) {
				clazzStr = fieldType.getName();
			}
			b.append(",").append(fieldName).append(":").append(clazzStr);
		}
		return b.toString();
	}

	public boolean containsFieldName(String fieldName) {
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
	      if (pretty) gen.useDefaultPrettyPrinter();
	      toJson(gen);
	      gen.flush();
	      return writer.toString();
	    } catch (IOException e) {
	      throw new RuntimeException(e);
	    }
	}
	

	/** Parse a schema from the provided string.
   * If named, the schema is added to the names known to this parser. */
  public static Schema parse(String s) {
    try {
      return parse(FACTORY.createJsonParser(new StringReader(s)));
    } catch (IOException e) {
      throw new SchemaParseException(e);
    }
  }

  private static Schema parse(JsonParser parser) throws IOException {
    try {
      return Schema.parse(MAPPER.readTree(parser));
    } catch (JsonParseException e) {
      throw new IOException(e);
    } finally {
      
    }
  }
  
  static Schema parse(JsonNode schema) throws IOException {
  	String name = schema.get("name").getTextValue();
  	List<Field> fields = new ArrayList<Field>();
  	JsonNode fieldsNode =schema.get("fields"); 
  	while(fieldsNode.getElements().hasNext()){
  		JsonNode fieldNode = fieldsNode.getElements().next();
  		fields.add(Field.parse(fieldNode));
  		
  	}
  	return new Schema(name,fields);
  }

	@Override
	public boolean equals(Object o) {
		if(o instanceof Schema) {
			return toString().equals(o.toString());
		}
		return false;
	}
	
	
	void toJson(JsonGenerator gen) throws IOException {
    gen.writeStartObject();
    gen.writeStringField("name", name);
    gen.writeFieldName("fields");
    fieldsToJson(gen);
    gen.writeEndObject();
  }

  void fieldsToJson(JsonGenerator gen) throws IOException {
    gen.writeStartArray();
    for (Field f : fields) {
    	f.toJson(gen);
    }
    gen.writeEndArray();
  }
}
	
	

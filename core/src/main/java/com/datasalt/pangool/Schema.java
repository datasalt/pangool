package com.datasalt.pangool;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
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
			if (name == null){
				throw new IllegalArgumentException("Field name can't be null");
			}
			
			if (clazz == null){
				throw new IllegalArgumentException("Field type can't be null");
			}
			this.name = name;
			this.type = clazz;
		}

		public Class<?> getType() {
			return type;
		}

		public String getName() {
			return name;
		}
		
		public boolean equals(Object a){
			if (!(a instanceof Field)){
				return false;
			}
			Field that = (Field)a;
			return name.equals(that.getName()) && type.equals(that.getType());
		}
		
		public String toString() {
			try {
	      StringWriter writer = new StringWriter();
	      JsonGenerator gen = FACTORY.createJsonGenerator(writer);
	      toJson(gen);
	      gen.flush();
	      return writer.toString();
	    } catch (IOException e) {
	      throw new RuntimeException(e);
	    }
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
      gen.writeStringField("name", getName());
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
			this.indexByFieldName.put(field.getName(), index);
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
  	Iterator<JsonNode> fieldsNodes = fieldsNode.getElements();
  	while(fieldsNodes.hasNext()){
  		JsonNode fieldNode = fieldsNodes.next();
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
	
	

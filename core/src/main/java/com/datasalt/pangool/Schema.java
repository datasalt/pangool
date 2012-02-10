package com.datasalt.pangool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.BidiMap;
import org.apache.commons.collections.bidimap.DualHashBidiMap;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.codehaus.jackson.JsonFactory;
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
	}

	private List<Field> fields;
	private String name;

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
		this.fields = new ArrayList<Field>();
		this.fields.addAll(fields);
		this.fields = Collections.unmodifiableList(this.fields);
		
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
		return serialize();
	}

	public static Schema parse(String serialized) throws CoGrouperException /*, InvalidFieldException */{
		
		try {
			if(serialized == null || serialized.isEmpty()) {
				return null;
			}
			String[] tokens =serialized.split(";");
			String name = tokens[0];
			String[] fieldsStr = tokens[1].split(",");
			List<Field> fields = new ArrayList<Field>();
			for(String field : fieldsStr) {
				String[] nameType = field.split(":");
				if(nameType.length != 2) {
					throw new CoGrouperException("Incorrect fields description " + serialized);
				}
				String fieldName = nameType[0].trim();
				String fieldType = nameType[1].trim();
				fields.add(new Field(fieldName, strToClass(fieldType)));
			}
			return new Schema(name,fields);
		} catch(ClassNotFoundException e) {
			throw new CoGrouperException(e);
		}
	}

	@Override
	public boolean equals(Object o) {
		if(o instanceof Schema) {
			return toString().equals(o.toString());
		}
		return false;
	}
}
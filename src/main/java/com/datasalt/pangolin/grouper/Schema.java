package com.datasalt.pangolin.grouper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datasalt.pangolin.commons.CommonUtils;

public class Schema {
	private List<String> fieldNames;
	private List<Class> fieldTypes;

	private static final Map<String,Class> strToClazz=new HashMap<String,Class>();
	private static final Map<Class,String> clazzToStr;
	
	
	static {
		strToClazz.put("int",Integer.class);
		strToClazz.put("long",Long.class);
		strToClazz.put("float",Float.class);
		strToClazz.put("double",Double.class);
		strToClazz.put("string",String.class);
		strToClazz.put("boolean",Boolean.class);
		
		clazzToStr = CommonUtils.invertMap(strToClazz);
	}
	
	/**
	 * TODO
	 * 
	 * @param str
	 * @return
	 */
	public static Class strToClass(String str) {
		return strToClazz.get(str);
	}
	
	public static String classToStr(Class clazz) {
		return clazzToStr.get(clazz);
	}
	
	
	private Map<String, Integer> indexByFieldName = new HashMap<String, Integer>();

	public Schema(List<String> fieldNames, List<Class> fieldTypes) {
		if (fieldNames.size() != fieldTypes.size()) {
			throw new RuntimeException("Field names size (" + fieldNames.size()
					+ ") doesn't match fieldTypes  (" + fieldTypes.size());
		}
		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
	}

	public List<String> getFieldNames() {
		return fieldNames;
	}

	public List<Class> getFieldTypes() {
		return fieldTypes;
	}

	public String getFieldName(int index) {
		return fieldNames.get(index);
	}

	public Class getFieldType(int index) {
		return fieldTypes.get(index);
	}

	public int getNumFields() {
		return fieldNames.size();
	}

	public String serialize() {
		StringBuilder b = new StringBuilder();
		String fieldName = fieldNames.get(0);
		Class fieldType = fieldTypes.get(0);
		b.append(fieldName).append(":").append(classToStr(fieldType));
		for (int i = 1 ; i < fieldTypes.size() ; i++){
			fieldName = fieldNames.get(i);
			fieldType = fieldTypes.get(i);
			b.append(",").append(fieldName).append(":").append(classToStr(fieldType));
		}
		return b.toString();
	}
	
	public String toString(){
		return serialize();
	}

	public static Schema parse(String serialized) {
		// TODO super wip
		List<String> fieldNames = new ArrayList<String>();
		List<Class> fieldTypes = new ArrayList<Class>();

		String[] fields = serialized.split(",");
		for (String field : fields) {
			String[] nameType = field.split(":");
			String name = nameType[0];
			String type = nameType[1];
			fieldNames.add(name);
			fieldTypes.add(strToClass(type));
		}
		return new Schema(fieldNames, fieldTypes);
	}


}

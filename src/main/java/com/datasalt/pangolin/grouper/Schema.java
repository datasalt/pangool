package com.datasalt.pangolin.grouper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;

import com.datasalt.pangolin.commons.CommonUtils;

public class Schema {
	
	public static class Field {
		public static enum SortCriteria {
			ASC,DESC
		}
		private String name;
		private Class type;
		private SortCriteria sortCriteria;
		
		public Field(String name,Class clazz){
			this.name = name;
			this.type = clazz;
			this.sortCriteria = SortCriteria.ASC;
		}
		
		public Field(String name,Class clazz,SortCriteria sort){
			this.name = name;
			this.type = clazz;
			this.sortCriteria = sort;
		}
		
		
		public Class getType(){
			return type;
		}
		
		public String getName(){
			return name;
		}
		
		public SortCriteria getSortCriteria(){
			return sortCriteria;
		}
	}
	
	private static final Map<String,Class> strToClazz=new HashMap<String,Class>();
	private static final Map<Class,String> clazzToStr;
	
	
	static {
		strToClazz.put("int",Integer.class);
		strToClazz.put("vint", VIntWritable.class);
		strToClazz.put("long",Long.class);
		strToClazz.put("vlong",VLongWritable.class);
		strToClazz.put("float",Float.class);
		strToClazz.put("double",Double.class);
		strToClazz.put("string",String.class);
		strToClazz.put("boolean",Boolean.class);
		
		clazzToStr = CommonUtils.invertMap(strToClazz);
	}
	
	private Field[] fields;
	
	
	
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

	public Schema(Field[] fields) {
		this.fields = fields;
		int index=0;
		for (Field field : fields){
			this.indexByFieldName.put(field.getName(),index);
			index++;
		}
		
	}
	
	public Field[] getFields(){
		return fields;
	}

	public String serialize() {
		StringBuilder b = new StringBuilder();
		String fieldName = fields[0].name;
		Class fieldType = fields[0].type;
		b.append(fieldName).append(":").append(classToStr(fieldType));
		for (int i = 1 ; i < fields.length ; i++){
			fieldName = fields[i].name;
			fieldType = fields[i].type;
			//TODO add sort criteria
			b.append(",").append(fieldName).append(":").append(classToStr(fieldType));
		}
		return b.toString();
	}
	
	public int getIndexByFieldName(String name){
		return indexByFieldName.get(name);
	}
	
	
	public String toString(){
		return serialize();
	}

	public static Schema parse(String serialized) {
		String[] fieldsStr = serialized.split(",");
		List<Field> fields = new ArrayList<Field>(fieldsStr.length);
		for (String field : fieldsStr) {
			String[] nameType = field.split(":");
			String name = nameType[0];
			String type = nameType[1];
			fields.add(new Field(name,strToClass(type)));
		}
		Field[] fieldsArray = new Field[fields.size()];
		fields.toArray(fieldsArray);
		return new Schema(fieldsArray);
	}


}

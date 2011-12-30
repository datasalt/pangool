/**
 * Copyright [2011] [Datasalt Systems S.L.]
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
package com.datasalt.pangolin.grouper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;

import com.datasalt.pangolin.grouper.io.tuple.ITuple;

/**
 * 
 * {@link FieldsDescription} specifies which fields (name and types) a {@link ITuple} will contain.
 * 
 * @author eric
 *
 */
public class FieldsDescription {
	private static final String CONF_SCHEMA = "datasalt.grouper.schema";
	
	public static class Field {
		private String name;
		private Class<?> type;
		
		public Field(){
			
		}
		
		public Field(String name,Class<?> clazz){
			this.name = name;
			this.type = clazz;
		}
		
		public Field(String name,Class<?> clazz,SortCriteria sort){
			this.name = name;
			this.type = clazz;
		}
		
		public Class<?> getType(){
			return type;
		}
		
		public String getName(){
			return name;
		}
		
	}
	
	private static final Map<String,Class<?>> strToClazz=new HashMap<String,Class<?>>();
	private static final Map<Class<?>,String> clazzToStr=new HashMap<Class<?>,String>();
	
	
	static {
		strToClazz.put("int",Integer.class);
		clazzToStr.put(Integer.class,"int");
		
		strToClazz.put("vint", VIntWritable.class);
		clazzToStr.put(VIntWritable.class,"vint");
		
		strToClazz.put("long",Long.class);
		clazzToStr.put(Long.class,"long");
		
		strToClazz.put("vlong",VLongWritable.class);
		clazzToStr.put(VLongWritable.class,"vlong");
		
		strToClazz.put("float",Float.class);
		clazzToStr.put(Float.class,"float");
		
		strToClazz.put("double",Double.class);
		clazzToStr.put(Double.class,"double");
		
		strToClazz.put("string",String.class);
		clazzToStr.put(String.class,"string");
		
		strToClazz.put("boolean",Boolean.class);
		clazzToStr.put(Boolean.class,"boolean");
		
	}
	
	private Field[] fields;
	
	public static Class<?> strToClass(String str) throws ClassNotFoundException {
		Class<?> clazz =strToClazz.get(str); 
		if (clazz == null){
			clazz = Class.forName(str);
		}
		return clazz;
	}
	
	public static String classToStr(Class<?> clazz) {
		return clazzToStr.get(clazz);
	}
	
	
	private Map<String, Integer> indexByFieldName = new HashMap<String, Integer>();

	public FieldsDescription(Field[] fields) {
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
	
	public Field getField(String fieldName){
		int index=indexByFieldName(fieldName);
		return fields[index];
	}
	
	public Field getField(int i){
		return fields[i];
	}
	

	public String serialize() {
		StringBuilder b = new StringBuilder();
		String fieldName = fields[0].name;
		Class<?> fieldType = fields[0].type;
		b.append(fieldName).append(":").append(classToStr(fieldType));
		for (int i = 1 ; i < fields.length ; i++){
			fieldName = fields[i].name;
			fieldType = fields[i].type;
			String clazzStr = classToStr(fieldType);
			if (clazzStr == null){
				clazzStr = fieldType.getName();
			}
			b.append(",").append(fieldName).append(":").append(clazzStr);
		}
		return b.toString();
	}
	
	public boolean containsFieldName(String fieldName){
		return indexByFieldName.containsKey(fieldName);
	}
	
	public int indexByFieldName(String name){
		return indexByFieldName.get(name);
	}
	
	@Override
	public String toString(){
		return serialize();
	}

	public static FieldsDescription parse(String serialized) throws GrouperException {
		try{
		if (serialized == null || serialized.isEmpty()){
			return null;
		}
		String[] fieldsStr = serialized.split(",");
		List<Field> fields = new ArrayList<Field>(fieldsStr.length);
		for (String field : fieldsStr) {
			String[] nameType = field.split(":");
			if (nameType.length != 2){
				throw new GrouperException("Incorrect fields description " +  serialized);
			}
			String name = nameType[0].trim();
			String type = nameType[1].trim();
			fields.add(new Field(name,strToClass(type)));
		}
		Field[] fieldsArray = new Field[fields.size()];
		fields.toArray(fieldsArray);
		return new FieldsDescription(fieldsArray);
		} catch(ClassNotFoundException e){
			throw new GrouperException(e);
		}
	}
	
	/**
	 * Parses serialized schema from configuration {@link Configuration}
	 * @param conf
	 * @return
	 * @throws GrouperException
	 */
	public static FieldsDescription parse(Configuration conf) throws GrouperException{
		String schemaStr = conf.get(FieldsDescription.CONF_SCHEMA);
		return FieldsDescription.parse(schemaStr);
	}
	
	public static void setInConfig(FieldsDescription schema,Configuration conf){
		conf.set(CONF_SCHEMA,schema.serialize());
	}
	
	@Override
	public boolean equals(Object o){
		if (o instanceof FieldsDescription ){
			return toString().equals(o.toString());
		}
		return false;
	}
}

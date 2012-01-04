package com.datasalt.pangolin.grouper;
///**
// * Copyright [2011] [Datasalt Systems S.L.]
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package com.datasalt.pangolin.grouper;
//
//import java.io.IOException;
//import java.text.ParseException;
//import java.util.Map;
//
//import org.codehaus.jackson.JsonGenerationException;
//import org.codehaus.jackson.JsonParseException;
//import org.codehaus.jackson.map.JsonMappingException;
//import org.codehaus.jackson.map.ObjectMapper;
//
//import com.datasalt.pangolin.grouper.Schema.Field;
//import com.datasalt.pangolin.grouper.SortCriteria.SortElement;
//
//public class Schema {
//	
//	public Map<Integer, Schema> getFieldsDescriptionBySource() {
//  	return fieldsDescriptionBySource;
//  }
//
//
//	public SortCriteria getSortCriteria() {
//  	return sortCriteria;
//  }
//
//	private Map<Integer,Schema> fieldsDescriptionBySource;
//	private SortCriteria sortCriteria;
//	
//	Schema(Map<Integer,Schema> fieldsDescriptionBySource,SortCriteria sortCriteria) throws GrouperException{
//		this.fieldsDescriptionBySource = fieldsDescriptionBySource;
//		this.sortCriteria = sortCriteria;
//		check();
//	}
//	
//	
//	
//	private void check() throws GrouperException {
//		for (SortElement sortElement : sortCriteria.getSortElements()){
//			String nameToFind = sortElement.getName();
//			for (Schema fieldsDescription : fieldsDescriptionBySource.values()){
//				boolean found=false;
//				for (Field field : fieldsDescription.getFields()){
//					if (field.getName().equals(nameToFind)){
//						found = true;
//						break;
//					}
//				}
//				if (!found){
//					throw new GrouperException ("Field name : " + nameToFind + " not found");
//				}
//			}
//		}
//	}
//	
//	
//	public String toJson() throws JsonGenerationException, JsonMappingException, IOException{
//		ObjectMapper mapper = new ObjectMapper();
//		String str = mapper.writeValueAsString(this);
//		return str;
//	}
//	
//	public static Schema parseJson(String json) throws JsonParseException, JsonMappingException, IOException{
//		ObjectMapper mapper = new ObjectMapper();
//		Schema schema = mapper.readValue(json,Schema.class);
//		return schema;
//		
//	}
//	
//	public static Schema parse(String format) throws ParseException{
//		String[] lines = format.split("\n+"); //non empty lines
//		//String line
//		//TODO
//		return null;
//	}
//	
//	
//	@Override
//	public String toString(){
//		try{
//		return toJson();
//		} catch(Exception e){
//			throw new RuntimeException(e);
//		}
//	}
//}

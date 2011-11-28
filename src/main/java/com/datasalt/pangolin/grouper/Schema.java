package com.datasalt.pangolin.grouper;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.datasalt.pangolin.grouper.FieldsDescription.Field;
import com.datasalt.pangolin.grouper.SortCriteria.SortElement;

public class Schema {
	
	public Map<Integer, FieldsDescription> getFieldsDescriptionBySource() {
  	return fieldsDescriptionBySource;
  }


	public SortCriteria getSortCriteria() {
  	return sortCriteria;
  }

	private Map<Integer,FieldsDescription> fieldsDescriptionBySource;
	private SortCriteria sortCriteria;
	
	Schema(Map<Integer,FieldsDescription> fieldsDescriptionBySource,SortCriteria sortCriteria) throws GrouperException{
		this.fieldsDescriptionBySource = fieldsDescriptionBySource;
		this.sortCriteria = sortCriteria;
		check();
	}
	
	
	
	private void check() throws GrouperException {
		for (SortElement sortElement : sortCriteria.getSortElements()){
			String nameToFind = sortElement.getName();
			for (FieldsDescription fieldsDescription : fieldsDescriptionBySource.values()){
				boolean found=false;
				for (Field field : fieldsDescription.getFields()){
					if (field.getName().equals(nameToFind)){
						found = true;
						break;
					}
				}
				if (!found){
					throw new GrouperException ("Field name : " + nameToFind + " not found");
				}
			}
		}
	}
	
	
	public String toJson() throws JsonGenerationException, JsonMappingException, IOException{
		ObjectMapper mapper = new ObjectMapper();
		String str = mapper.writeValueAsString(this);
		return str;
	}
	
	public static Schema parseJson(String json) throws JsonParseException, JsonMappingException, IOException{
		ObjectMapper mapper = new ObjectMapper();
		Schema schema = mapper.readValue(json,Schema.class);
		return schema;
		
	}
	
	public static Schema parse(String format) throws ParseException{
		String[] lines = format.split("\n+"); //non empty lines
		//String line
		//TODO
		return null;
	}
	
	
	@Override
	public String toString(){
		try{
		return toJson();
		} catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}

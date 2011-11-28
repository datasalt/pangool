package com.datasalt.pangolin.grouper;

import java.util.HashMap;
import java.util.Map;

public class SchemaBuilder {

	private Map<Integer,FieldsDescription> fieldsDescriptionBySource = new HashMap<Integer,FieldsDescription>();
	private SortCriteria sortCriteria;
	
	public SchemaBuilder(){
		
	}
	
	public void setFieldsDescriptionForSource(int source,String fieldsDescription) throws GrouperException{
		FieldsDescription f = FieldsDescription.parse(fieldsDescription);
		fieldsDescriptionBySource.put(source,f);
	}
	
	public void setSortCriteria(String sortCriteria) throws GrouperException{
		this.sortCriteria = SortCriteria.parse(sortCriteria);
	}
	
	public Schema createSchema() throws GrouperException{
		  return new Schema(fieldsDescriptionBySource,sortCriteria);
	}
	
}

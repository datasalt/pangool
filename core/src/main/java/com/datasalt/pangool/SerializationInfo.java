package com.datasalt.pangool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.Schema.Field;
import com.datasalt.pangool.SortCriteria.SortElement;

public class SerializationInfo {

	private CoGrouperConfig grouperConfig;
	private Schema commonSchema; //if multiple sources used , then the source field is always the last one.
	private Map<Integer,Schema> specificSchemas; 
	
	public SerializationInfo(CoGrouperConfig grouperConfig) throws CoGrouperException{
		this.grouperConfig = grouperConfig;
		initializeAll();
	}
	
	private void initializeAll() throws CoGrouperException{
		Sorting sorting = grouperConfig.getSorting();
		SortCriteria commonSortCriteria = sorting.getCommonSortCriteria();
		
		List<Field> commonFields = new ArrayList<Field>();
		boolean containsSourceField = false;
		for (SortElement sortElement : commonSortCriteria.getSortElements()){
			String fieldName = sortElement.getFieldName();
			if (!containsSourceField){
				containsSourceField = fieldName.equals(grouperConfig.getSourceField());
			} else {
				throw new CoGrouperException("Source field '" + fieldName+ "' must be the last field in common sort criteria in Sorting");
			}
			Class<?> type = checkFieldInAllSources(fieldName);
			commonFields.add(new Field(fieldName,type));
		}
		
		if (!containsSourceField){
			Class<?> sourceFieldType = checkFieldInAllSources(grouperConfig.getSourceField()); 
			commonFields.add(new Field(grouperConfig.getSourceField(),sourceFieldType));
		}
		this.commonSchema = new Schema("common",commonFields);
		this.specificSchemas = new HashMap<Integer,Schema>();
		Map<Integer,List<Field>> specificFieldsBySource = new HashMap<Integer,List<Field>>();
		
		for (Map.Entry<Integer,SortCriteria> entry : sorting.getSpecificSortCriterias().entrySet()){
			List<Field> specificFields = new ArrayList<Field>();
			SortCriteria specificCriteria = entry.getValue(); int sourceId = entry.getKey();
			for (SortElement sortElement : specificCriteria.getSortElements()){
				String fieldName = sortElement.getFieldName();
				Class<?> fieldType = checkFieldInSource(fieldName, sourceId);
				specificFields.add(new Field(fieldName,fieldType));
			}
			specificFieldsBySource.put(sourceId,specificFields);
		}
		
		for (Map.Entry<Integer,Schema> entry : grouperConfig.getSources().entrySet()){
			Schema sourceSchema = entry.getValue(); int sourceId = entry.getKey();
			List<Field> specificFields = specificFieldsBySource.get(sourceId);
			for (Field field : sourceSchema.getFields()){
				if (!commonSchema.containsFieldName(field.name()) && !containsFieldName(field.name(),specificFields)){
					specificFields.add(field);
				}
			}
			this.specificSchemas.put(sourceId,new Schema("specific",specificFields));
		}
		
	}
	
	private boolean containsFieldName(String fieldName,List<Field> fields){
		for (Field field : fields){
			if (field.name().equals(fieldName)){
				return true;
			}
		}
		return false;
	}
	
	private Class<?> checkFieldInAllSources(String name) throws CoGrouperException{
		Class<?> type = null;
		for (Integer sourceId: grouperConfig.getSources().keySet()){
			Class<?> typeInSource = checkFieldInSource(name,sourceId);
			if (type == null){
				type = typeInSource;
			} else if (type != typeInSource){
				throw new CoGrouperException("The type for field '"+ name + "' is not the same in all the sources");
			}
		}
		return type;
	}
	
	private Class<?> checkFieldInSource(String name,int sourceId) throws CoGrouperException{
		Schema schema = grouperConfig.getSource(sourceId);
		Field field =schema.getField(name); 
		if (field == null){
			throw new CoGrouperException("Field '" + name + "' not present in source '" + sourceId + "' " + schema);
		} 
		return field.type();
	}
		
		
	
	
	public Schema getCommonSchema(){
		return commonSchema;
	}
	
	public Schema getSpecificSchema(int sourceId){
		return specificSchemas.get(sourceId);
	}
	
	public Map<Integer,Schema> getSpecificSchemas(){
		return specificSchemas;
	}
	
}

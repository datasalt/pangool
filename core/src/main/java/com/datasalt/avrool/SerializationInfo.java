package com.datasalt.avrool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Field.Order;


import com.datasalt.avrool.Ordering.SortElement;
import com.datasalt.avrool.io.AvroUtils;

public class SerializationInfo {

		//public static final String REGULAR_NAMESPACE="com.datasalt";
	public static final String REGULAR_NAMESPACE=null;	
	public static final String INTERMEDIATE_SCHEMA_NAME ="intermediate";
		
		
		public static final String UNION_FIELD_NAME = "our_union";
	//	public static final String UNION_FIELD_NAMESPACE="uf_namespace";
		
		Schema commonSchema;
		Map<String,Schema> particularSchemas = new LinkedHashMap<String,Schema>();
		Order interSourcesOrder;
		
		SerializationInfo(){}
		
		public Schema getCommonSchema(){
			return commonSchema;
		}
		
		public Schema getParticularSchema(String source){
			return particularSchemas.get(source);
		}
		
		public Map<String,Schema> getParticularSchemas(){
			return particularSchemas;
		}
		
		public Schema getIntermediateSchema(){
			List<Schema> unionSchemas = new ArrayList<Schema>();
			for (Map.Entry<String, Schema> entry : particularSchemas.entrySet()){
				System.out.println("INTERMEDIATE SCHEMA : " + entry.getKey() + "=> " + entry.getValue().getFullName());
				
			}
			
			unionSchemas.addAll(particularSchemas.values());
			List<Field> fields = new ArrayList<Field>();
			for (Field commonField : commonSchema.getFields()){
				fields.add(AvroUtils.cloneField(commonField));
			}
			
			Field unionField =new Field(UNION_FIELD_NAME,Schema.createUnion(unionSchemas),null,null,interSourcesOrder); 
			fields.add(unionField);
			
			System.out.println("INTERMEDIATE SCHEMA : " + unionField.schema().getTypes());
			
			
			Schema result = Schema.createRecord(INTERMEDIATE_SCHEMA_NAME,null,REGULAR_NAMESPACE,false);
			result.setFields(fields);
			return result;
		}
	
	
	
	public static SerializationInfo get(CoGrouperConfig conf) throws CoGrouperException{

		SerializationInfo result = new SerializationInfo();
		result.interSourcesOrder = conf.interSourcesOrdering;
		List<Field> commonSchemaFields = new ArrayList<Field>();
		
		for (SortElement element : conf.commonOrdering.getElements()){
			String fieldName = element.getName();
			Schema schemaType = null;
			for (Map.Entry<String,Schema> entry : conf.schemasBySource.entrySet()){
				String sourceName = entry.getKey();
				Schema sourceSchema = entry.getValue();
				Field field = sourceSchema.getField(fieldName);
				if (field == null){
					throw new CoGrouperException("Field '" + "' not present in all sources.In source '" + sourceName + "' is missing "); 
				}
				Schema currentSchema =field.schema(); 
				if (schemaType == null){
					schemaType = currentSchema;
				} else if (!schemaType.equals(currentSchema)){
					throw new CoGrouperException("The schema for field '" + fieldName + "' is not the same in all sources");
				}
			}
			commonSchemaFields.add(new Field(fieldName,schemaType,null,null,element.getOrder()));
		}
		
		result.commonSchema = Schema.createRecord(commonSchemaFields);
		
		//initializing particular schemas with empty fields
		Map<String,List<Field>> particularFields = new HashMap<String,List<Field>>();
		for (Map.Entry<String,Schema> entry : conf.schemasBySource.entrySet()){
			Schema s = Schema.createRecord(entry.getKey(), null,REGULAR_NAMESPACE,false);
			//System.out.println("Particular schema : " + s.getFullName());
			result.particularSchemas.put(entry.getKey(),s);
			particularFields.put(entry.getKey(), new ArrayList<Field>());
		}
		
		
		//adding sortable fields for every particular schema
		for (Map.Entry<String,Ordering> entries : conf.particularOrderings.entrySet()){
			String sourceName = entries.getKey();
			Ordering order = entries.getValue();
			Schema sourceSchema = conf.schemasBySource.get(sourceName);
			if (sourceSchema == null){
				throw new CoGrouperException("Particular sorting by source '" + sourceName +"' incorrect. No source '" + sourceName + "' declared");
			}
			
			for (SortElement element : order.getElements()){
				String fieldName = element.getName();
				Field schemaField = sourceSchema.getField(fieldName);
				if (schemaField == null){
					throw new CoGrouperException("Field '" + fieldName + "' not present in source '" + sourceName + "'");
				}
				if (result.commonSchema.getField(fieldName) != null){
					throw new CoGrouperException("Field '"+ fieldName + "' is already present in common ordering. Not allowed to particular sort by this field");
				}
				particularFields.get(sourceName).add(new Field(fieldName,schemaField.schema(),null,null,element.getOrder()));
			}
		}
		
		
		// adding the rest of the fields for every particular schema
		
		for (Map.Entry<String,Schema> entry : conf.schemasBySource.entrySet()){
			String source = entry.getKey();
			Schema sourceSchema = entry.getValue();
			
			List<Field> alreadySetFields = particularFields.get(source);
			for (Field f : sourceSchema.getFields()){
				if (result.commonSchema.getField(f.name()) == null && !containsField(alreadySetFields,f.name())){
					Field fieldWithNoOrder = new Field(f.name(),f.schema(),f.doc(),f.defaultValue(),Order.IGNORE); //TODO this doesn't avoid comparison of further fields
					alreadySetFields.add(fieldWithNoOrder);
				}
			}
		}
		
		//finally assigning fields for particular schemas 
		for (Map.Entry<String,List<Field>> entry : particularFields.entrySet()){
			result.particularSchemas.get(entry.getKey()).setFields(entry.getValue());
		}
		
		return result;
		
	}
	
	
	private static boolean containsField(List<Field> fields, String name){
		for (Field f : fields){
			if (f.name().equals(name)){
				return true;
			}
		}
		
		return false;
	}
	
	
}

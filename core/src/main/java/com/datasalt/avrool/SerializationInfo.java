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
import com.datasalt.avrool.commons.AvroUtils;

public class SerializationInfo {

	public static final String REGULAR_NAMESPACE=null;	
	public static final String INTERMEDIATE_SCHEMA_NAME ="intermediate";
	public static final String UNION_FIELD_NAME = "our_union";
		
		Schema commonSchema;
		Schema groupSchema;
		Schema partitionerSchema;
		CoGrouperConfig grouperConfig;
		
		Map<String,Schema> particularSchemas = new LinkedHashMap<String,Schema>();
		Order interSourcesOrder;
		
		SerializationInfo(){}
		
		public Schema getCommonSchema(){
			return commonSchema;
		}
		
		public Schema getGroupSchema(){
			return groupSchema;
		}
		
		public Schema getPartitionerSchema(){
			return partitionerSchema;
		}
		
		
		public Schema getParticularSchema(String source){
			return particularSchemas.get(source);
		}
		
		public Map<String,Schema> getParticularSchemas(){
			return particularSchemas;
		}
		
		public Schema getIntermediateSchema(){
			
			List<Field> fields = new ArrayList<Field>();
			for (Field commonField : commonSchema.getFields()){
				fields.add(AvroUtils.cloneField(commonField));
			}
			
			if (particularSchemas != null && !particularSchemas.isEmpty()){
				List<Schema> unionSchemas = new ArrayList<Schema>();
				unionSchemas.addAll(particularSchemas.values());
				Field unionField =new Field(UNION_FIELD_NAME,Schema.createUnion(unionSchemas),null,null,interSourcesOrder); 
				fields.add(unionField);
			}
//			} else if (particularSchemas.size() == 1){
//				Schema particularSchema = particularSchemas.values().iterator().next();
//				for (Field commonField : particularSchema.getFields()){
//					fields.add(AvroUtils.cloneField(commonField));
//				}
//				
//			}
			Schema result = Schema.createRecord(INTERMEDIATE_SCHEMA_NAME,null,REGULAR_NAMESPACE,false);
			result.setFields(fields);
			return result;
		}
		
		public static class PositionMapping {
			
			public Map<String,int[]> commonTranslation;
			public Map<String,int[]> particularTranslation;
			
			PositionMapping(Map<String,int[]> commonTranslation, Map<String,int[]> particularTranslation){
				this.commonTranslation = commonTranslation;
				this.particularTranslation = particularTranslation;
			}
			
			@Override
			public String toString(){
				StringBuilder b = new StringBuilder();
				
				for (Map.Entry<String,int[]> entry : commonTranslation.entrySet()){
					String source = entry.getKey();
					int[] currentArray = entry.getValue();
					b.append("common source:["+source +"]=>");
					for (int i=0 ; i < currentArray.length ; i++){
						b.append(currentArray[i]).append(",");
					}
					b.append("\n");
				
				}
				
				for (Map.Entry<String,int[]> entry : particularTranslation.entrySet()){
					String source = entry.getKey();
					int[] currentArray = entry.getValue();
					b.append("particular source:["+source +"]=>");
					for (int i=0 ; i < currentArray.length ; i++){
						b.append(currentArray[i]).append(",");
					}
					b.append("\n");
				}
				
				
				return b.toString();
			}
		}
		
		
		
		public PositionMapping getMapperTranslation(){
			int numCommonFields = commonSchema.getFields().size();
			Map<String,int[]> commonTranslation = new HashMap<String,int[]>();
			Map<String,int[]> particularTranslation = new HashMap<String,int[]>();
			
			Schema interSchema = getIntermediateSchema();
			for (Map.Entry<String,Schema> entry : grouperConfig.getSchemasBySource().entrySet()){
				String sourceName = entry.getKey();
				Schema sourceSchema = entry.getValue();
				commonTranslation.put(sourceName,new int[numCommonFields]);
				
				Schema particularSchema = null;
				if (particularSchemas != null && !particularSchemas.isEmpty()){
					particularSchema = particularSchemas.get(sourceName);
					particularTranslation.put(sourceName,new int[particularSchema.getFields().size()]);
				}
				for (Field f: sourceSchema.getFields()){
					int posSource = f.pos();
					Field interField =interSchema.getField(f.name());
					if (interField != null){
						int posInter = interField.pos();
						commonTranslation.get(sourceName)[posInter] = posSource;
					} else if (particularSchema != null){
						int posParticular = particularSchema.getField(f.name()).pos();
						particularTranslation.get(sourceName)[posParticular] = posSource; 
					}
				}
			}
			
			return new PositionMapping(commonTranslation, particularTranslation);
		}
		
		
		public PositionMapping getReducerTranslation(){

			Map<String,int[]> commonTranslation = new HashMap<String,int[]>();
			Map<String,int[]> particularTranslation = new HashMap<String,int[]>();
			
			Schema interSchema = getIntermediateSchema();
			for (Map.Entry<String,Schema> entry : grouperConfig.getSchemasBySource().entrySet()){
				String sourceName = entry.getKey();
				Schema sourceSchema = entry.getValue();
				commonTranslation.put(sourceName,new int[sourceSchema.getFields().size()]);
				
				Schema particularSchema = null;
				if (particularSchemas != null && !particularSchemas.isEmpty()){
					particularSchema = particularSchemas.get(sourceName);
					particularTranslation.put(sourceName,new int[sourceSchema.getFields().size()]);
				}
				for (Field f: sourceSchema.getFields()){
					int posSource = f.pos();
					Field interField =interSchema.getField(f.name());
					if (interField != null){
						int posInter = interField.pos();
						commonTranslation.get(sourceName)[posSource] = posInter;
					} else if (particularSchema != null){
						commonTranslation.get(sourceName)[posSource] = -1;
						int posParticular = particularSchema.getField(f.name()).pos();
						particularTranslation.get(sourceName)[posSource] = posParticular; 
					}
				}
			}
			return new PositionMapping(commonTranslation, particularTranslation);
		}
		
		
		
	private static SerializationInfo buildMonoSource(CoGrouperConfig conf) throws CoGrouperException {
		SerializationInfo result = new SerializationInfo();
		result.grouperConfig = conf;
		result.interSourcesOrder = conf.interSourcesOrdering;
		//TODO check that the fields in groupBy are in common ordering
		List<Field> groupFields = new ArrayList<Field>();
		List<Field> partitionerFields = new ArrayList<Field>();
		List<Field> commonSchemaFields = new ArrayList<Field>();

		Schema sourceSchema = conf.schemasBySource.values().iterator().next();
		 
		
		for (SortElement element : conf.commonOrdering.getElements()){
			String fieldName = element.getName();
			Field field = sourceSchema.getField(fieldName);
			if (field == null){
				throw new CoGrouperException("Field '" + fieldName + "' not present in source.Source:"+ sourceSchema ); 
			}
			Schema  schemaType=field.schema(); 
			commonSchemaFields.add(new Field(fieldName,schemaType,null,null,element.getOrder()));
			if (conf.getGroupByFields().contains(fieldName)){
				groupFields.add(new Field(fieldName,schemaType,null,null,element.getOrder()));
			}
			if (conf.getRollupBaseFields().contains(fieldName)){
				partitionerFields.add(new Field(fieldName,schemaType,null,null,element.getOrder()));
			}
		}
		
		for (Field f : sourceSchema.getFields()){
			if (!containsField(commonSchemaFields, f.name())){
				commonSchemaFields.add(new Field(f.name(),f.schema(),null,null,Order.IGNORE));
			}
		}
		result.commonSchema = Schema.createRecord(commonSchemaFields);
		result.groupSchema = Schema.createRecord(groupFields);
		result.partitionerSchema = Schema.createRecord(partitionerFields);
		result.particularSchemas = null;
		return result;
		
	}
	
	public static SerializationInfo get(CoGrouperConfig grouperConfig) throws CoGrouperException{
		if (grouperConfig.getNumSources() >= 2){
			return buildMultipleSources(grouperConfig);
		} else {
			return buildMonoSource(grouperConfig);
		}
	}
	
	
	private static SerializationInfo buildMultipleSources(CoGrouperConfig conf) throws CoGrouperException{

		SerializationInfo result = new SerializationInfo();
		result.grouperConfig = conf;
		result.interSourcesOrder = conf.interSourcesOrdering;
		//TODO check that the fields in groupBy are in common ordering
		List<Field> groupFields = new ArrayList<Field>();
		List<Field> partitionerFields = new ArrayList<Field>();
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
			if (conf.getGroupByFields().contains(fieldName)){
				groupFields.add(new Field(fieldName,schemaType,null,null,element.getOrder()));
			}
			
			if (conf.getRollupBaseFields().contains(fieldName)){
				partitionerFields.add(new Field(fieldName,schemaType,null,null,element.getOrder()));
			}
			
			
		}
		
		result.commonSchema = Schema.createRecord(commonSchemaFields);
		result.groupSchema = Schema.createRecord(groupFields);
		result.partitionerSchema = Schema.createRecord(partitionerFields);
		
		
		
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
	
	public static int[] getIdentityArray(int size){
		int[] result = new int[size];
		for (int i=0 ; i < size ; i++){
			result[i] = i;
		}
		return result;
	}
	
	
}

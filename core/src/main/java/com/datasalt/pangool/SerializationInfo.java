package com.datasalt.pangool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datasalt.pangool.SortBy.SortElement;
import com.datasalt.pangool.Schema.Field;

public class SerializationInfo {

	private CoGrouperConfig grouperConfig;
	private Schema commonSchema;
	private List<Schema> specificSchemas;
	private List<int[]> fieldsToPartition=new ArrayList<int[]>();
	private Schema groupSchema;
	
	public SerializationInfo(CoGrouperConfig grouperConfig) throws CoGrouperException{
		this.grouperConfig = grouperConfig;
		if (grouperConfig.getNumSources() >= 2){
			initializeMultipleSources();
		} else {
			initializeOneSource();
		}
	}
	
	private void initializeOneSource() throws CoGrouperException {
		calculateOneSourceCommonSchema();
		calculatePartitionFields();
		calculateGroupSchema();
	}
	
	private void initializeMultipleSources() throws CoGrouperException{
		calculateMultipleSourcesIntermediateSchemas();
		calculatePartitionFields();
		calculateGroupSchema();
	}

	public List<int[]> getFieldsToPartition(){
		return fieldsToPartition;
	}
	
	private void calculateGroupSchema(){
		List<Field> fields = commonSchema.getFields();
		List<Field> groupFields = fields.subList(0, grouperConfig.getGroupByFields().size());
		this.groupSchema = new Schema("group",groupFields);
	}
	
	private void calculatePartitionFields() {
		List<String> partitionFields = grouperConfig.getRollupBaseFields();
		int numFields = partitionFields.size();
		for (Schema schema : grouperConfig.getSourceSchemas()){
			int[] posFields = new int[numFields];
			for (int i = 0 ; i < partitionFields.size(); i++){
				int pos = schema.getFieldPos(partitionFields.get(i));
				posFields[i]=pos;
			}
			fieldsToPartition.add(posFields);
		}
	}
	
	private void calculateOneSourceCommonSchema() throws CoGrouperException {
		Schema sourceSchema =grouperConfig.getSourceSchemas().get(0); 
		
		SortBy commonSortCriteria = grouperConfig.getCommonSortBy();
		List<Field> commonFields = new ArrayList<Field>();
		for (SortElement sortElement : commonSortCriteria.getElements()){
			String fieldName = sortElement.getName();
			Class<?> type = checkFieldInAllSources(fieldName);
			commonFields.add(new Field(fieldName,type));
		}

		//add particular fields if any..
		SortBy particularOrderBy = grouperConfig.getSecondarySortBys().get(0);
		for (SortElement sortElement : particularOrderBy.getElements()){
			String fieldName = sortElement.getName();
			Class<?> fieldType = checkFieldInSource(fieldName, 0);
			commonFields.add(new Field(fieldName,fieldType));
		}
		
		//adding the rest
			for (Field field : sourceSchema.getFields()){
				if (!containsFieldName(field.name(),commonFields)){
					commonFields.add(field);
				}
			}
		this.commonSchema = new Schema("common",commonFields);
	}
	
	
	private void calculateMultipleSourcesIntermediateSchemas() throws CoGrouperException {
		SortBy commonSortCriteria = grouperConfig.getCommonSortBy();
		List<Field> commonFields = new ArrayList<Field>();
		for (SortElement sortElement : commonSortCriteria.getElements()){
			String fieldName = sortElement.getName();
			Class<?> type = checkFieldInAllSources(fieldName);
			commonFields.add(new Field(fieldName,type));
		}

		this.commonSchema = new Schema("common",commonFields);
		this.specificSchemas = new ArrayList<Schema>();
		List<List<Field>> specificFieldsBySource = new ArrayList<List<Field>>();
		
		
		for (int sourceId=0 ; sourceId < grouperConfig.getNumSources(); sourceId++){
			SortBy specificCriteria = grouperConfig.getSecondarySortBys().get(sourceId);
			List<Field> specificFields = new ArrayList<Field>();
			for (SortElement sortElement : specificCriteria.getElements()){
				String fieldName = sortElement.getName();
				Class<?> fieldType = checkFieldInSource(fieldName, sourceId);
				specificFields.add(new Field(fieldName,fieldType));
			}
			specificFieldsBySource.add(specificFields);
		}
		
		for (int i= 0 ; i < grouperConfig.getNumSources(); i++){
			Schema sourceSchema = grouperConfig.getSourceSchema(i);
			List<Field> specificFields = specificFieldsBySource.get(i);
			for (Field field : sourceSchema.getFields()){
				if (!commonSchema.containsFieldName(field.name()) && !containsFieldName(field.name(),specificFields)){
					specificFields.add(field);
				}
			}
			this.specificSchemas.add(new Schema("specific",specificFields));
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
		for (int i=0 ; i < grouperConfig.getSourceSchemas().size() ; i++){
			Class<?> typeInSource = checkFieldInSource(name,i);
			if (type == null){
				type = typeInSource;
			} else if (type != typeInSource){
				throw new CoGrouperException("The type for field '"+ name + "' is not the same in all the sources");
			}
		}
		return type;
	}
	
	private Class<?> checkFieldInSource(String fieldName,int sourceId ) throws CoGrouperException{
		Schema schema = grouperConfig.getSourceSchema(sourceId);
		Field field =schema.getField(fieldName); 
		if (field == null){
			throw new CoGrouperException("Field '" + fieldName + "' not present in source '" +  schema.getName() + "' " + schema);
		} 
		return field.getType();
	}
		
	public Schema getCommonSchema(){
		return commonSchema;
	}
	
	public Schema getSpecificSchema(int sourceId){
		return specificSchemas.get(sourceId);
	}
	
	public List<Schema> getSpecificSchemas(){
		return specificSchemas;
	}
	
	public Schema getGroupSchema(){
		return groupSchema;
	}
	
	
	public static class PositionMapping {
		
		public List<int[]> commonTranslation;
		public List<int[]> particularTranslation;
		
		PositionMapping(List<int[]> commonTranslation, List<int[]> particularTranslation){
			this.commonTranslation = commonTranslation;
			this.particularTranslation = particularTranslation;
		}
		
		@Override
		public String toString(){
			StringBuilder b = new StringBuilder();
			
			int source=0;
			for (int[] currentArray : commonTranslation){
				b.append("common source:["+source +"]=>");
				for (int i=0 ; i < currentArray.length ; i++){
					b.append(currentArray[i]).append(",");
				}
				b.append("\n");
				source++;
			}
			
			source=0;
			for (int[] currentArray : particularTranslation){
				b.append("particular source:["+source +"]=>");
				for (int i=0 ; i < currentArray.length ; i++){
					b.append(currentArray[i]).append(",");
				}
				b.append("\n");
				source++;
			}
			
			
			return b.toString();
		}
	}
	
	
	
	public PositionMapping getSerializationTranslation(){
		List<int[]> commonTranslation = new ArrayList<int[]>();
		List<int[]> particularTranslation = new ArrayList<int[]>();
		for (int sourceId = 0 ; sourceId < grouperConfig.getSourceSchemas().size() ; sourceId++){
			Schema sourceSchema = grouperConfig.getSourceSchema(sourceId);
			commonTranslation.add(getIndexTranslation(commonSchema,sourceSchema));
			Schema particularSchema = null;
			if (specificSchemas != null && !specificSchemas.isEmpty()){
				particularSchema = specificSchemas.get(sourceId);
				particularTranslation.add(getIndexTranslation(particularSchema,sourceSchema));
			}
		}
		return new PositionMapping(commonTranslation, particularTranslation);
	}
	
	
	/**
	 * 
	 * @param source The result length will match the source fields size
	 * @param dest The resulting array will contain indexes to this destination schema
	 * @return The translation index array
	 */
	public static final int[] getIndexTranslation(Schema source,Schema dest){
		int[] result = new int[source.getFields().size()];
		for (int i=0 ; i < result.length ; i++){
			String fieldName = source.getField(i).name();
			int destPos = dest.getFieldPos(fieldName);
			result[i] = destPos;
		}
		return result;
	}
	
}

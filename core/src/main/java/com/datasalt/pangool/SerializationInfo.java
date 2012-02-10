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
	private Map<String,Schema> specificSchemas;
	private Map<String,Integer> sourceIdsByName;
	private Map<String,int[]> fieldsToPartition=new HashMap<String,int[]>();
	private String[] sourceNames;
	private Schema groupSchema;
	
	public SerializationInfo(CoGrouperConfig grouperConfig) throws CoGrouperException{
		this.grouperConfig = grouperConfig;
		if (grouperConfig.getNumSources() >= 2){
			initializeMultipleSources();
		} else {
			initializeOneSource();
		}
	}
	
	public int getSourceIdByName(String sourceName){
		return sourceIdsByName.get(sourceName);
	}
	
	public String getSourceNameById(int sourceId){
		return sourceNames[sourceId];
	}
	
	private void initializeOneSource() throws CoGrouperException {
		calculateOneSourceCommonSchema();
		calculatePartitionFields();
		calculateGroupSchema();
	}
	
	private void initializeMultipleSources() throws CoGrouperException{
		calculateSourceIdsByName();
		calculateMultipleSourcesIntermediateSchemas();
		calculatePartitionFields();
		calculateGroupSchema();
	}

	public Map<String,int[]> getFieldsToPartition(){
		return fieldsToPartition;
	}
	
	private void calculateGroupSchema(){
		List<Field> fields = commonSchema.getFields();
		List<Field> groupFields = fields.subList(0, grouperConfig.getGroupByFields().size());
		this.groupSchema = new Schema("group",groupFields);
	}
	
	private void calculateSourceIdsByName(){
		sourceIdsByName = new HashMap<String,Integer>();
		int sourceId=0;
		sourceNames = new String[grouperConfig.getNumSources()];
		for (Map.Entry<String,Schema> entry : grouperConfig.getSourceSchemas().entrySet()	){
			sourceNames[sourceId] = entry.getKey();
			sourceIdsByName.put(entry.getKey(), sourceId);
			sourceId++;
		}
	}
	
	private void calculatePartitionFields() {
		List<String> partitionFields = grouperConfig.getRollupBaseFields();
		int numFields = partitionFields.size();
		for (Map.Entry<String,Schema> entry : grouperConfig.getSourceSchemas().entrySet()){
			String sourceName = entry.getKey();
			Schema schema = entry.getValue();
			int[] posFields = new int[numFields];
			for (int i = 0 ; i < partitionFields.size(); i++){
				int pos = schema.getFieldPos(partitionFields.get(i));
				posFields[i]=pos;
			}
			fieldsToPartition.put(sourceName,posFields);
		}
	}
	
	private void calculateOneSourceCommonSchema() throws CoGrouperException {
		Schema sourceSchema =grouperConfig.getSourceSchemas().values().iterator().next(); 
		String sourceName = sourceSchema.getName();
		
		SortBy commonSortCriteria = grouperConfig.getCommonSortBy();
		List<Field> commonFields = new ArrayList<Field>();
		for (SortElement sortElement : commonSortCriteria.getElements()){
			String fieldName = sortElement.getName();
			Class<?> type = checkFieldInAllSources(fieldName);
			commonFields.add(new Field(fieldName,type));
		}

		//add particular fields if any..
		SortBy particularOrderBy = grouperConfig.getSecondarySortBys().get(sourceName);
		for (SortElement sortElement : particularOrderBy.getElements()){
			String fieldName = sortElement.getName();
			Class<?> fieldType = checkFieldInSource(fieldName, sourceName);
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
		this.specificSchemas = new HashMap<String,Schema>();
		Map<String,List<Field>> specificFieldsBySource = new HashMap<String,List<Field>>();
		
		for (Map.Entry<String,SortBy> entry : grouperConfig.getSecondarySortBys().entrySet()){
			List<Field> specificFields = new ArrayList<Field>();
			SortBy specificCriteria = entry.getValue(); String sourceName = entry.getKey();
			for (SortElement sortElement : specificCriteria.getElements()){
				String fieldName = sortElement.getName();
				Class<?> fieldType = checkFieldInSource(fieldName, sourceName);
				specificFields.add(new Field(fieldName,fieldType));
			}
			specificFieldsBySource.put(sourceName,specificFields);
		}
		
		for (Map.Entry<String,Schema> entry : grouperConfig.getSourceSchemas().entrySet()){
			Schema sourceSchema = entry.getValue(); String sourceName = entry.getKey();
			List<Field> specificFields = specificFieldsBySource.get(sourceName);
			for (Field field : sourceSchema.getFields()){
				if (!commonSchema.containsFieldName(field.name()) && !containsFieldName(field.name(),specificFields)){
					specificFields.add(field);
				}
			}
			this.specificSchemas.put(sourceName,new Schema("specific",specificFields));
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
		for (String sourceName: grouperConfig.getSourceSchemas().keySet()){
			Class<?> typeInSource = checkFieldInSource(name,sourceName);
			if (type == null){
				type = typeInSource;
			} else if (type != typeInSource){
				throw new CoGrouperException("The type for field '"+ name + "' is not the same in all the sources");
			}
		}
		return type;
	}
	
	private Class<?> checkFieldInSource(String fieldName,String sourceName ) throws CoGrouperException{
		Schema schema = grouperConfig.getSourceSchema(sourceName);
		Field field =schema.getField(fieldName); 
		if (field == null){
			throw new CoGrouperException("Field '" + fieldName + "' not present in source '" +  sourceName + "' " + schema);
		} 
		return field.getType();
	}
		
	public Schema getCommonSchema(){
		return commonSchema;
	}
	
	public Schema getSpecificSchema(String sourceName){
		return specificSchemas.get(sourceName);
	}
	
	public Map<String,Schema> getSpecificSchemas(){
		return specificSchemas;
	}
	
	public Schema getGroupSchema(){
		return groupSchema;
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
	
	
	
	public PositionMapping getSerializationTranslation(){
		Map<String,int[]> commonTranslation = new HashMap<String,int[]>();
		Map<String,int[]> particularTranslation = new HashMap<String,int[]>();
		for (Map.Entry<String,Schema> entry : grouperConfig.getSourceSchemas().entrySet()){
			String sourceName = entry.getKey();
			Schema sourceSchema = entry.getValue();
			commonTranslation.put(sourceName,getIndexTranslation(commonSchema,sourceSchema));
			
			Schema particularSchema = null;
			if (specificSchemas != null && !specificSchemas.isEmpty()){
				particularSchema = specificSchemas.get(sourceName);
				particularTranslation.put(sourceName,getIndexTranslation(particularSchema,sourceSchema));
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

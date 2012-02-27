/**
 * Copyright [2012] [Datasalt Systems S.L.]
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
package com.datasalt.pangool.cogroup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.datasalt.pangool.cogroup.sorting.Criteria;
import com.datasalt.pangool.cogroup.sorting.Criteria.SortElement;
import com.datasalt.pangool.io.tuple.Schema;
import com.datasalt.pangool.io.tuple.Schema.Field;
import com.datasalt.pangool.mapreduce.Partitioner;
import com.datasalt.pangool.mapreduce.RollupReducer;
import com.datasalt.pangool.mapreduce.SimpleReducer;
import com.datasalt.pangool.mapreduce.SortComparator;
import com.datasalt.pangool.serialization.tuples.PangoolSerialization;

/**
 * 
 * Contains information about how to perform binary internal serialization and comparison.
 * This is used ,among others, in {@link PangoolSerialization} , {@link Partitioner} , {@link SortComparator},
 * as well {@link SimpleReducer} and {@link RollupReducer}. 
 */
public class SerializationInfo {

	private final TupleMRConfig grouperConfig;
	/* Fields that will be ser/deserialized before finding sourceId */
	private Schema commonSchema; 
	
	/*Fields,for every source,that will be ser/deserialized after the sourceId.*/
	private List<Schema> specificSchemas; 
	
	/** 
	 * Fields that define a group. It matches the fields defined in 
	 * {@link TupleMRConfig#getGroupByFields()} ordered accordingly {@link TupleMRConfig#getCommonCriteria()} 
	 */ 
	private Schema groupSchema;
	
	
	private List<int[]> fieldsToPartition=new ArrayList<int[]>();
	private List<int[]> commonToSourcesIndexes=new ArrayList<int[]>();
	private List<int[]> groupToSourcesIndexes=new ArrayList<int[]>();
	private List<int[]> specificToSourcesIndexes=new ArrayList<int[]>();
	
	
	
	public SerializationInfo(TupleMRConfig grouperConfig) throws TupleMRException{
		this.grouperConfig = grouperConfig;
		if (grouperConfig.getNumIntermediateSchemas() >= 2){
			initializeMultipleSources();
		} else {
			initializeOneSource();
		}
	}
	
	private void initializeOneSource() throws TupleMRException {
		calculateOneSourceCommonSchema();
		calculatePartitionFields();
		calculateGroupSchema();
		calculateIndexTranslations();
	}
	
	private void initializeMultipleSources() throws TupleMRException{
		calculateMultipleSourcesIntermediateSchemas();
		calculatePartitionFields();
		calculateGroupSchema();
		calculateIndexTranslations();
	}

	public List<int[]> getPartitionFieldsIndexes(){
		return fieldsToPartition;
	}
	
	public int[] getFieldsToPartition(int sourceId){
		return fieldsToPartition.get(sourceId);
	}
	
	public int[] getCommonSchemaIndexTranslation(int sourceId){
		return commonToSourcesIndexes.get(sourceId);
	}
	
	public int[] getSpecificSchemaIndexTranslation(int sourceId){
		return specificToSourcesIndexes.get(sourceId);
	}
	
	public int[] getGroupSchemaIndexTranslation(int sourceId){
		return groupToSourcesIndexes.get(sourceId);
	}
	
	
	private void calculateGroupSchema(){
		List<Field> fields = commonSchema.getFields();
		List<Field> groupFields = fields.subList(0, grouperConfig.getGroupByFields().size());
		this.groupSchema = new Schema("group",groupFields);
	}
	
	private void calculatePartitionFields() {
		List<String> partitionFields;
		if (grouperConfig.getCustomPartitionFields() != null){
			partitionFields = grouperConfig.getCustomPartitionFields();
		} else {
			partitionFields = grouperConfig.calculateRollupBaseFields();
		}
		int numFields = partitionFields.size();
		for (Schema schema : grouperConfig.getIntermediateSchemas()){
			int[] posFields = new int[numFields];
			for (int i = 0 ; i < partitionFields.size(); i++){
				int pos = schema.getFieldPos(partitionFields.get(i));
				posFields[i]=pos;
			}
			fieldsToPartition.add(posFields);
		}
	}
	
	private void calculateOneSourceCommonSchema() throws TupleMRException {
		Schema sourceSchema =grouperConfig.getIntermediateSchemas().get(0); 
		
		Criteria commonSortCriteria = grouperConfig.getCommonCriteria();
		List<Field> commonFields = new ArrayList<Field>();
		for (SortElement sortElement : commonSortCriteria.getElements()){
			String fieldName = sortElement.getName();
			Field field = checkFieldInAllSources(fieldName);
			commonFields.add(field);
		}

		//adding the rest
			for (Field field : sourceSchema.getFields()){
				if (!containsFieldName(field.getName(),commonFields)){
					commonFields.add(field);
				}
			}
		this.commonSchema = new Schema("common",commonFields);
	}
	
	
	private void calculateMultipleSourcesIntermediateSchemas() throws TupleMRException {
		Criteria commonSortCriteria = grouperConfig.getCommonCriteria();
		List<Field> commonFields = new ArrayList<Field>();
		for (SortElement sortElement : commonSortCriteria.getElements()){
			String fieldName = sortElement.getName();
			Field field = checkFieldInAllSources(fieldName);
			commonFields.add(field);
		}

		this.commonSchema = new Schema("common",commonFields);
		this.specificSchemas = new ArrayList<Schema>();
		List<List<Field>> specificFieldsBySource = new ArrayList<List<Field>>();
		
		for (int sourceId=0 ; sourceId < grouperConfig.getNumIntermediateSchemas(); sourceId++){
			Criteria specificCriteria = grouperConfig.getSecondarySortBys().get(sourceId);
			List<Field> specificFields = new ArrayList<Field>();
			if (specificCriteria != null){
				for (SortElement sortElement : specificCriteria.getElements()){
					String fieldName = sortElement.getName();
					Field field = checkFieldInSource(fieldName, sourceId);
					specificFields.add(field);
				}
			}
			specificFieldsBySource.add(specificFields);
		}
		
		for (int i= 0 ; i < grouperConfig.getNumIntermediateSchemas(); i++){
			Schema sourceSchema = grouperConfig.getIntermediateSchema(i);
			List<Field> specificFields = specificFieldsBySource.get(i);
//			if (specificFields == null){
//				specificFields = new ArrayList<Field>();
//			}
			for (Field field : sourceSchema.getFields()){
				if (!commonSchema.containsField(field.getName()) && !containsFieldName(field.getName(),specificFields)){
					specificFields.add(field);
				}
			}
			this.specificSchemas.add(new Schema("specific",specificFields));
			
		}
		this.specificSchemas = Collections.unmodifiableList(this.specificSchemas);
	}
	
	
	
	private boolean containsFieldName(String fieldName,List<Field> fields){
		for (Field field : fields){
			if (field.getName().equals(fieldName)){
				return true;
			}
		}
		return false;
	}
	
	private Field checkFieldInAllSources(String name) throws TupleMRException{
		Field field = null;
		for (int i=0 ; i < grouperConfig.getIntermediateSchemas().size() ; i++){
			Field fieldInSource = checkFieldInSource(name,i);
			if (field == null){
				field = fieldInSource;
			} else if (!field.equals(fieldInSource)){
				throw new TupleMRException("The type for field '"+ name + "' is not the same in all the sources");
			}
		}
		return field;
	}
	
	private Field checkFieldInSource(String fieldName,int sourceId ) throws TupleMRException{
		Schema schema = grouperConfig.getIntermediateSchema(sourceId);
		Field field =schema.getField(fieldName); 
		if (field == null){
			throw new TupleMRException("Field '" + fieldName + "' not present in source '" +  schema.getName() + "' " + schema);
		} 
		return field;
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
	
	private void calculateIndexTranslations(){ 
		for (int sourceId = 0 ; sourceId < grouperConfig.getIntermediateSchemas().size() ; sourceId++){
			Schema sourceSchema = grouperConfig.getIntermediateSchema(sourceId);
			commonToSourcesIndexes.add(getIndexTranslation(commonSchema,sourceSchema));
			groupToSourcesIndexes.add(getIndexTranslation(groupSchema,sourceSchema));
			if (specificSchemas != null && !specificSchemas.isEmpty()){
				Schema particularSchema = specificSchemas.get(sourceId);
				specificToSourcesIndexes.add(getIndexTranslation(particularSchema,sourceSchema));
			}
		}
		commonToSourcesIndexes = Collections.unmodifiableList(commonToSourcesIndexes);
		groupToSourcesIndexes = Collections.unmodifiableList(groupToSourcesIndexes);
		specificToSourcesIndexes = Collections.unmodifiableList(specificToSourcesIndexes);
	}
	
	
	/**
	 * 
	 * @param source The result length will match the source schema fields size
	 * @param dest The resulting array will contain indexes to this destination schema
	 * @return The translation index array
	 */
	public static final int[] getIndexTranslation(Schema source,Schema dest){
		int[] result = new int[source.getFields().size()];
		for (int i=0 ; i < result.length ; i++){
			String fieldName = source.getField(i).getName();
			int destPos = dest.getFieldPos(fieldName);
			result[i] = destPos;
		}
		return result;
	}
	
}

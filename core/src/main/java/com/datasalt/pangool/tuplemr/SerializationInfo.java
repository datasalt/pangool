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
package com.datasalt.pangool.tuplemr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.tuplemr.Criteria.SortElement;
import com.datasalt.pangool.tuplemr.mapred.RollupReducer;
import com.datasalt.pangool.tuplemr.mapred.SimpleReducer;
import com.datasalt.pangool.tuplemr.mapred.SortComparator;
import com.datasalt.pangool.tuplemr.mapred.TupleHashPartitioner;
import com.datasalt.pangool.tuplemr.serialization.TupleSerialization;

/**
 * 
 * Contains information about how to perform binary internal serialization and
 * comparison. <p>
 * 
 * This is used ,among others, in {@link TupleSerialization} ,
 * {@link TupleHashPartitioner} , {@link SortComparator}, as well {@link SimpleReducer}
 * and {@link RollupReducer}.
 */
public class SerializationInfo {

	private final TupleMRConfig mrConfig;
	/* Fields that will be ser/deserialized before finding schema id */
	private Schema commonSchema;

	/* Fields,for every source,that will be ser/deserialized after the schema id. */
	private List<Schema> specificSchemas;

	/**
	 * Fields that define a group. It matches the fields defined in
	 * {@link TupleMRConfig#getGroupByFields()} ordered accordingly to
	 * {@link TupleMRConfig#getCommonCriteria()}
	 */
	private Schema groupSchema;

	private List<int[]> fieldsToPartition = new ArrayList<int[]>();
	private List<int[]> commonToIntermediateIndexes = new ArrayList<int[]>();
	private List<int[]> groupToIntermediateIndexes = new ArrayList<int[]>();
	private List<int[]> specificToIntermediateIndexes = new ArrayList<int[]>();

	public SerializationInfo(TupleMRConfig tupleMRConfig) throws TupleMRException {
		this.mrConfig = tupleMRConfig;
		if(tupleMRConfig.getNumIntermediateSchemas() >= 2) {
			initializeMultipleSources();
		} else {
			initializeOneSource();
		}
	}

	private void initializeOneSource() throws TupleMRException {
		calculateOneIntermediateCommonSchema();
		calculatePartitionFields();
		calculateGroupSchema();
		calculateIndexTranslations();
	}

	private void initializeMultipleSources() throws TupleMRException {
		calculateMultipleSourcesSubSchemas();
		calculatePartitionFields();
		calculateGroupSchema();
		calculateIndexTranslations();
	}

	public List<int[]> getPartitionFieldsIndexes() {
		return fieldsToPartition;
	}

	/**
	 * Given a schema returns the fields (indexes) that will be used to calculate
	 * a partial hashing by {@link TupleHashPartitioner}
	 * 
	 */
	public int[] getFieldsToPartition(int schemaId) {
		return fieldsToPartition.get(schemaId);
	}

	/**
	 * Given a intermediate schema id, returns an index correlation from common
	 * schema indexes to the specified intermediate schema indexes. The length of
	 * this array matches the number of fields in the common schema.
	 * 
	 */
	public int[] getCommonSchemaIndexTranslation(int schemaId) {
		return commonToIntermediateIndexes.get(schemaId);
	}

	/**
	 * Given a intermediate schema id, returns an index correlation from the
	 * specific schema to the intermediate schema. The length of this array
	 * matches the number of fields in the specific schema.
	 * 
	 */
	public int[] getSpecificSchemaIndexTranslation(int schemaId) {
		return specificToIntermediateIndexes.get(schemaId);
	}

	/**
	 * Given a intermediate schema id, returns an index correlation from the group
	 * schema to the intermediate schema. The length of this array matches the
	 * number of fields in the group schema.
	 * 
	 */
	public int[] getGroupSchemaIndexTranslation(int schemaId) {
		return groupToIntermediateIndexes.get(schemaId);
	}

	private void calculateGroupSchema() {
		List<Field> fields = commonSchema.getFields();
		List<Field> groupFields = fields.subList(0, mrConfig.getGroupByFields().size());
		this.groupSchema = new Schema("group", groupFields);
	}

	
	
	private void calculatePartitionFields() {
		List<String> partitionFields;
		if(!mrConfig.getCustomPartitionFields().isEmpty()) {
			partitionFields = mrConfig.getCustomPartitionFields();
		} else {
			partitionFields = mrConfig.calculateRollupBaseFields();
		}
		int numFields = partitionFields.size();
		for(Schema schema : mrConfig.getIntermediateSchemas()) {
			int[] posFields = new int[numFields];
			for(int i = 0; i < partitionFields.size(); i++) {
				int pos = getFieldPosUsingAliases(schema,partitionFields.get(i));
				posFields[i] = pos;
			}
			fieldsToPartition.add(posFields);
		}
	}

	private void calculateOneIntermediateCommonSchema() throws TupleMRException {
		Schema intermediateSchema = mrConfig.getIntermediateSchemas().get(0);
		Criteria commonSortCriteria = mrConfig.getCommonCriteria();
		List<Field> commonFields = new ArrayList<Field>();
		for(SortElement sortElement : commonSortCriteria.getElements()) {
			String fieldName = sortElement.getName();
			Field field = checkFieldInAllSchemas(fieldName);
			commonFields.add(Field.cloneField(field, fieldName));
		}

		// adding the rest
		for(Field field : intermediateSchema.getFields()) {
			Map<String,String> aliases = mrConfig.getFieldAliases(intermediateSchema.getName());
			if(!containsField(field.getName(),commonFields,aliases))  {
					commonFields.add(field);
			}
		}
		this.commonSchema = new Schema("common", commonFields);
	}

	private void calculateMultipleSourcesSubSchemas() throws TupleMRException {
		Criteria commonSortCriteria = mrConfig.getCommonCriteria();
		List<Field> commonFields = new ArrayList<Field>();
		for(SortElement sortElement : commonSortCriteria.getElements()) {
			String fieldName = sortElement.getName();
			Field field = checkFieldInAllSchemas(fieldName);
			
			commonFields.add(Field.cloneField(field,fieldName));
		}

		this.commonSchema = new Schema("common", commonFields);
		this.specificSchemas = new ArrayList<Schema>();
		List<List<Field>> specificFieldsBySource = new ArrayList<List<Field>>();

		for(int schemaId = 0; schemaId < mrConfig.getNumIntermediateSchemas(); schemaId++) {
			Criteria specificCriteria = mrConfig.getSpecificOrderBys().get(schemaId);
			List<Field> specificFields = new ArrayList<Field>();
			if(specificCriteria != null) {
				for(SortElement sortElement : specificCriteria.getElements()) {
					String fieldName = sortElement.getName();
					Field field = checkFieldInSchema(fieldName, schemaId);
					specificFields.add(Field.cloneField(field,fieldName));
				}
			}
			specificFieldsBySource.add(specificFields);
		}

		for(int i = 0; i < mrConfig.getNumIntermediateSchemas(); i++) {
			Schema sourceSchema = mrConfig.getIntermediateSchema(i);
			List<Field> specificFields = specificFieldsBySource.get(i);
			// TODO FIXME this shouldn't work. Use aliases
			for(Field field : sourceSchema.getFields()) {
				Map<String,String> sourceAliases = mrConfig.getFieldAliases(sourceSchema.getName());
				if(!containsField(field.getName(),commonSchema.getFields(),sourceAliases)
				    && !containsField(field.getName(), specificFields,sourceAliases)) {
					specificFields.add(field);
				}
			}
			this.specificSchemas.add(new Schema("specific", specificFields));
		}
		this.specificSchemas = Collections.unmodifiableList(this.specificSchemas);
	}

	
	private boolean containsField(String fieldName, List<Field> fields,
				Map<String,String> aliases) {
		for(Field field : fields) {
			if(fieldName.equals(field.getName()) || 
					(aliases != null && fieldName.equals(aliases.get(field.getName())))) {
				return true;
			}
		}
		return false;
	}

	private Field checkFieldInAllSchemas(String name) throws TupleMRException {
		Field field = null;
		for(int i = 0; i < mrConfig.getIntermediateSchemas().size(); i++) {
			Field fieldInSource = checkFieldInSchema(name, i);
			if(field == null) {
				field = fieldInSource;
			} else if(field.getType() != fieldInSource.getType() || field.getObjectClass() != fieldInSource.getObjectClass()) {
				throw new TupleMRException("The type for field '" + name
				    + "' is not the same in all the sources");
			}
		}
		return field;
	}

	private Field checkFieldInSchema(String fieldName, int schemaId)
	    throws TupleMRException {
		Schema schema = mrConfig.getIntermediateSchema(schemaId);
		Field field = getFieldUsingAliases(schema,fieldName);
		if(field == null) {
			throw new TupleMRException("Field '" + fieldName + "' not present in source '"
			    + schema.getName() + "' " + schema);
		}
		return field;
	}

	/**
	 * Returns the schema that contains fields that will be ser/deserialized
	 * before the schemaId. In case that one intermediate schema used then returns
	 * a schema containing all the fields from the provided intermediate schema
	 * with fields sorted by common criteria.
	 */
	public Schema getCommonSchema() {
		return commonSchema;
	}

	/**
	 * Given a intermediate schema id it returns a subschema from that
	 * intermediate schema that contains fields that will be serialized after the
	 * schemaId. Returns null if no fields are serialized after the schemaId.
	 */
	public Schema getSpecificSchema(int schemaId) {
		return specificSchemas.get(schemaId);
	}

	/**
	 * Returns a list containing all the specific schemas ordered by schema id.
	 * see {@link #getSpecificSchema}
	 * 
	 */
	public List<Schema> getSpecificSchemas() {
		return specificSchemas;
	}

	/**
	 * Returns the schema containing the group-by fields ordered by the common
	 * sorting criteria.
	 */
	public Schema getGroupSchema() {
		return groupSchema;
	}

	private void calculateIndexTranslations() {
		for(int schemaId = 0; schemaId < mrConfig.getIntermediateSchemas().size(); schemaId++) {
			Schema sourceSchema = mrConfig.getIntermediateSchema(schemaId);
			commonToIntermediateIndexes.add(getIndexTranslation(commonSchema, sourceSchema));
			groupToIntermediateIndexes.add(getIndexTranslation(groupSchema, sourceSchema));
			if(specificSchemas != null && !specificSchemas.isEmpty()) {
				Schema particularSchema = specificSchemas.get(schemaId);
				specificToIntermediateIndexes.add(getIndexTranslation(particularSchema,
				    sourceSchema));
			}
		}
		commonToIntermediateIndexes = Collections
		    .unmodifiableList(commonToIntermediateIndexes);
		groupToIntermediateIndexes = Collections.unmodifiableList(groupToIntermediateIndexes);
		specificToIntermediateIndexes = Collections
		    .unmodifiableList(specificToIntermediateIndexes);
	}

	/**
	 * 
	 * @param source
	 *          The result length will match the source schema fields size
	 * @param dest
	 *          The resulting array will contain indexes to this destination
	 *          schema
	 * @return The translation index array
	 */
	private  int[] getIndexTranslation(Schema source, Schema dest) {
		int[] result = new int[source.getFields().size()];
		for(int i = 0; i < result.length; i++) {
			String fieldName = source.getField(i).getName();
			int destPos = getFieldPosUsingAliases(dest,fieldName);
			result[i] = destPos;
		}
		return result;
	}
	
	private int getFieldPosUsingAliases(Schema schema,String field){
		Map<String,String> aliases = mrConfig.getFieldAliases(schema.getName());
		return Schema.getFieldPosUsingAliases(schema, field, aliases);
	}
	
	private Field getFieldUsingAliases(Schema schema,String field){
		Map<String,String> aliases = mrConfig.getFieldAliases(schema.getName());
		return Schema.getFieldUsingAliases(schema, field, aliases);
	}
	

}

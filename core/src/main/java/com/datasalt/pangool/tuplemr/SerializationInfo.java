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

import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.FieldConfigurable;
import com.datasalt.pangool.tuplemr.Criteria.SortElement;
import com.datasalt.pangool.tuplemr.mapred.RollupReducer;
import com.datasalt.pangool.tuplemr.mapred.SimpleReducer;
import com.datasalt.pangool.tuplemr.mapred.SortComparator;
import com.datasalt.pangool.tuplemr.mapred.TupleHashPartitioner;
import com.datasalt.pangool.tuplemr.serialization.TupleSerialization;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.util.ReflectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Contains information about how to perform binary internal serialization and
 * comparison. <p>
 * <p/>
 * This is used ,among others, in {@link TupleSerialization} ,
 * {@link TupleHashPartitioner} , {@link SortComparator}, as well {@link SimpleReducer}
 * and {@link RollupReducer}.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class SerializationInfo {

  private final TupleMRConfig mrConfig;
  /* Fields that will be hadoopSer/deserialized before finding schema id */
  private Schema commonSchema;

  /* Fields,for every source,that will be hadoopSer/deserialized after the schema id. */
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


  private Serializer[] commonSerializers;
  private Deserializer[] commonDeserializers;
  private List<Serializer[]> specificSerializers;
  private List<Deserializer[]> specificDeserializers;
  private Serializer[] groupSerializers;
  private Deserializer[] groupDeserializers;

  public SerializationInfo(TupleMRConfig tupleMRConfig) throws TupleMRException {
    this.mrConfig = tupleMRConfig;
    if (tupleMRConfig.getNumIntermediateSchemas() >= 2) {
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
    initCommonAndGroupSchemaSerialization();
  }

  private void initializeMultipleSources() throws TupleMRException {
    calculateMultipleSourcesSubSchemas();
    calculatePartitionFields();
    calculateGroupSchema();
    calculateIndexTranslations();
    initCommonAndGroupSchemaSerialization();
    initSpecificSchemaSerialization();
  }

  public List<int[]> getPartitionFieldsIndexes() {
    return fieldsToPartition;
  }

  /**
   * Given a schema returns the fields (indexes) that will be used to calculate
   * a partial hashing by {@link TupleHashPartitioner}
   */
  public int[] getFieldsToPartition(int schemaId) {
    return fieldsToPartition.get(schemaId);
  }

  /**
   * Given a intermediate schema id, returns an index correlation from common
   * schema indexes to the specified intermediate schema indexes. The length of
   * this array matches the number of fields in the common schema.
   */
  public int[] getCommonSchemaIndexTranslation(int schemaId) {
    return commonToIntermediateIndexes.get(schemaId);
  }

  /**
   * Given a intermediate schema id, returns an index correlation from the
   * specific schema to the intermediate schema. The length of this array
   * matches the number of fields in the specific schema.
   */
  public int[] getSpecificSchemaIndexTranslation(int schemaId) {
    return specificToIntermediateIndexes.get(schemaId);
  }

  /**
   * Given a intermediate schema id, returns an index correlation from the group
   * schema to the intermediate schema. The length of this array matches the
   * number of fields in the group schema.
   */
  public int[] getGroupSchemaIndexTranslation(int schemaId) {
    return groupToIntermediateIndexes.get(schemaId);
  }

  @SuppressWarnings("serial")
  private void calculateGroupSchema() {
    List<Field> fields = commonSchema.getFields();
    List<Field> groupFields = fields.subList(0, mrConfig.getGroupByFields().size());
    /*
     * IMPORTANT: Group schema must inherit the containsNullableFields() from the
     * common schema. Otherwise, code using the group schema for deserialize
     * (like comparators) would fail, as some cases the group schema could not
     * have null fields, but the common schema could have them.
     */
    this.groupSchema = new Schema("group", groupFields) {
      boolean containsNulls = commonSchema.containsNullableFields();

      @Override
      public boolean containsNullableFields() {
        return containsNulls;
      }
    };
  }

  public List<Serializer[]> getSpecificSchemaSerializers() {
    return specificSerializers;
  }

  public List<Deserializer[]> getSpecificSchemaDeserializers() {
    return specificDeserializers;
  }

  public Serializer[] getCommonSchemaSerializers() {
    return commonSerializers;
  }

  public Deserializer[] getCommonSchemaDeserializers() {
    return commonDeserializers;
  }

  public Serializer[] getGroupSchemaSerializers() {
    return groupSerializers;
  }

  public Deserializer[] getGroupSchemaDeserializers() {
    return groupDeserializers;
  }

  /**
   * This serializers have been defined by the user in an OBJECT field
   */
  private void initCommonAndGroupSchemaSerialization() {
    //TODO Should SerializationInfo contain Configuration ?
    commonSerializers = getSerializers(commonSchema, null);
    commonDeserializers = getDeserializers(commonSchema, commonSchema, null);

    groupSerializers = getSerializers(groupSchema, null);
    groupDeserializers = getDeserializers(groupSchema, groupSchema, null);
  }

  private void initSpecificSchemaSerialization() {
    specificSerializers = new ArrayList<Serializer[]>();
    specificDeserializers = new ArrayList<Deserializer[]>();
    for (int i = 0; i < specificSchemas.size(); i++) {
      Schema specificSchema = specificSchemas.get(i);
      //TODO Should SerializationInfo contain Configuration ?
      specificSerializers.add(getSerializers(specificSchema, null));
      specificDeserializers.add(getDeserializers(specificSchema, specificSchema, null));
    }
  }

  public static Serializer[] getSerializers(Schema schema, Configuration conf) {
    Serializer[] result = new Serializer[schema.getFields().size()];
    for (int i = 0; i < result.length; i++) {
      Field field = schema.getField(i);
      if (field.getObjectSerialization() != null) {
        Serialization serialization = ReflectionUtils.newInstance(field.getObjectSerialization(), conf);
        if (serialization instanceof FieldConfigurable) {
          ((FieldConfigurable) serialization).setFieldProperties(null, field.getProps());
        }
        result[i] = serialization.getSerializer(field.getObjectClass());
      }
    }
    return result;
  }

  public static Deserializer[] getDeserializers(Schema readSchema, Schema targetSchema, Configuration conf) {
    Deserializer[] result = new Deserializer[readSchema.getFields().size()];
    for (int i = 0; i < result.length; i++) {
      Field field = readSchema.getField(i);
      if (field.getObjectSerialization() == null) {
      	continue;
      }
      Serialization serialization = ReflectionUtils.newInstance(field.getObjectSerialization(), conf);
      if (serialization instanceof FieldConfigurable) {
      	Map<String, String> targetSchemaMetadata = null;
      	// Look if this field is also in the target Schema, so we extract both metadata
      	if(targetSchema.containsField(field.getName())) {
      		Field targetSchemaField = targetSchema.getField(field.getName());
      		if(targetSchemaField.getObjectSerialization() == null ||
      			!targetSchemaField.getObjectSerialization().equals(field.getObjectSerialization())) {
      			// Error: field in target schema with same name but different serialization mechanism!
      			throw new RuntimeException("Target schema has field [" + field.getName() + "] with different serialization than read schema field with same name.");
      		}
      		targetSchemaMetadata = targetSchemaField.getProps();
      	}
        ((FieldConfigurable) serialization).setFieldProperties(field.getProps(), targetSchemaMetadata);
      }
      result[i] = serialization.getDeserializer(field.getObjectClass());
    }
    return result;
  }

  private void calculatePartitionFields() {
    List<String> partitionFields;
    if (!mrConfig.getCustomPartitionFields().isEmpty()) {
      partitionFields = mrConfig.getCustomPartitionFields();
    } else {
      partitionFields = mrConfig.calculateRollupBaseFields();
    }
    int numFields = partitionFields.size();
    for (Schema schema : mrConfig.getIntermediateSchemas()) {
      int[] posFields = new int[numFields];
      for (int i = 0; i < partitionFields.size(); i++) {
        int pos = getFieldPosUsingAliases(schema, partitionFields.get(i));
        posFields[i] = pos;
      }
      fieldsToPartition.add(posFields);
    }
  }

  private void calculateOneIntermediateCommonSchema() throws TupleMRException {
    Schema intermediateSchema = mrConfig.getIntermediateSchemas().get(0);
    Criteria commonSortCriteria = mrConfig.getCommonCriteria();
    List<Field> commonFields = new ArrayList<Field>();
    for (SortElement sortElement : commonSortCriteria.getElements()) {
      String fieldName = sortElement.getName();
      Field field = checkFieldInAllSchemas(fieldName);
      commonFields.add(Field.cloneField(field, fieldName));
    }

    // adding the rest
    for (Field field : intermediateSchema.getFields()) {
      Map<String, String> aliases = mrConfig.getFieldAliases(intermediateSchema.getName());
      if (!containsField(field.getName(), commonFields, aliases)) {
        commonFields.add(field);
      }
    }
    this.commonSchema = new Schema("common", commonFields);
  }

  private void calculateMultipleSourcesSubSchemas() throws TupleMRException {
    Criteria commonSortCriteria = mrConfig.getCommonCriteria();
    List<Field> commonFields = new ArrayList<Field>();
    for (SortElement sortElement : commonSortCriteria.getElements()) {
      String fieldName = sortElement.getName();
      Field field = checkFieldInAllSchemas(fieldName);

      commonFields.add(Field.cloneField(field, fieldName));
    }

    this.commonSchema = new Schema("common", commonFields);
    this.specificSchemas = new ArrayList<Schema>();
    List<List<Field>> specificFieldsBySource = new ArrayList<List<Field>>();

    for (int schemaId = 0; schemaId < mrConfig.getNumIntermediateSchemas(); schemaId++) {
      Criteria specificCriteria = mrConfig.getSpecificOrderBys().get(schemaId);
      List<Field> specificFields = new ArrayList<Field>();
      if (specificCriteria != null) {
        for (SortElement sortElement : specificCriteria.getElements()) {
          String fieldName = sortElement.getName();
          Field field = checkFieldInSchema(fieldName, schemaId);
          specificFields.add(Field.cloneField(field, fieldName));
        }
      }
      specificFieldsBySource.add(specificFields);
    }

    for (int i = 0; i < mrConfig.getNumIntermediateSchemas(); i++) {
      Schema sourceSchema = mrConfig.getIntermediateSchema(i);
      List<Field> specificFields = specificFieldsBySource.get(i);
      for (Field field : sourceSchema.getFields()) {
        Map<String, String> sourceAliases = mrConfig.getFieldAliases(sourceSchema.getName());
        if (!containsField(field.getName(), commonSchema.getFields(), sourceAliases)
            && !containsField(field.getName(), specificFields, sourceAliases)) {
          specificFields.add(field);
        }
      }
      this.specificSchemas.add(new Schema("specific", specificFields));
    }
    this.specificSchemas = Collections.unmodifiableList(this.specificSchemas);
  }


  private boolean containsField(String fieldName, List<Field> fields,
                                Map<String, String> aliases) {
    for (Field field : fields) {
      if (fieldName.equals(field.getName()) ||
          (aliases != null && fieldName.equals(aliases.get(field.getName())))) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks that the field with the given name is in all schemas, and
   * select a representative field that will be used for serializing. In the case of
   * having a mixture of fields, some of them nullable and some others no nullables,
   * a nullable Field will be returned.
   */
  private Field checkFieldInAllSchemas(String name) throws TupleMRException {
    Field field = null;
    for (int i = 0; i < mrConfig.getIntermediateSchemas().size(); i++) {
      Field fieldInSource = checkFieldInSchema(name, i);
      if (field == null) {
        field = fieldInSource;
      } else if (field.getType() != fieldInSource.getType() || field.getObjectClass() != fieldInSource.getObjectClass()) {
        throw new TupleMRException("The type for field '" + name
            + "' is not the same in all the sources");
      } else if (fieldInSource.isNullable()) {
        // IMPORTANT CASE. Nullable fields must be returned when present nullable and non nullable fields mixed
        field = fieldInSource;
      }
    }
    return field;
  }

  private Field checkFieldInSchema(String fieldName, int schemaId)
      throws TupleMRException {
    Schema schema = mrConfig.getIntermediateSchema(schemaId);
    Field field = getFieldUsingAliases(schema, fieldName);
    if (field == null) {
      throw new TupleMRException("Field '" + fieldName + "' not present in source '"
          + schema.getName() + "' " + schema);
    }
    return field;
  }

  /**
   * Returns the schema that contains fields that will be hadoopSer/deserialized
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
    for (int schemaId = 0; schemaId < mrConfig.getIntermediateSchemas().size(); schemaId++) {
      Schema sourceSchema = mrConfig.getIntermediateSchema(schemaId);
      commonToIntermediateIndexes.add(getIndexTranslation(commonSchema, sourceSchema));
      groupToIntermediateIndexes.add(getIndexTranslation(groupSchema, sourceSchema));
      if (specificSchemas != null && !specificSchemas.isEmpty()) {
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
   * @param source The result length will match the source schema fields size
   * @param dest   The resulting array will contain indexes to this destination
   *               schema
   * @return The translation index array
   */
  private int[] getIndexTranslation(Schema source, Schema dest) {
    int[] result = new int[source.getFields().size()];
    for (int i = 0; i < result.length; i++) {
      String fieldName = source.getField(i).getName();
      int destPos = getFieldPosUsingAliases(dest, fieldName);
      result[i] = destPos;
    }
    return result;
  }

  private int getFieldPosUsingAliases(Schema schema, String field) {
    Map<String, String> aliases = mrConfig.getFieldAliases(schema.getName());
    return Schema.getFieldPosUsingAliases(schema, field, aliases);
  }

  private Field getFieldUsingAliases(Schema schema, String field) {
    Map<String, String> aliases = mrConfig.getFieldAliases(schema.getName());
    return Schema.getFieldUsingAliases(schema, field, aliases);
  }


}

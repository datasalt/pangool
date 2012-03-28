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

import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.Criteria.SortElement;
import com.datasalt.pangool.tuplemr.mapred.GroupComparator;
import com.datasalt.pangool.tuplemr.mapred.RollupReducer;
import com.datasalt.pangool.tuplemr.mapred.SortComparator;
import com.datasalt.pangool.utils.DCUtils;

/**
 * TupleMRConfig contains the entire configuration parameters from a Tuple-based
 * job. The main information that it contains :
 * <ul>
 * <li>Intermediate schemas</li>
 * <li>Group-by fields</li>
 * <li>Order-by common criteria</li>
 * <li>Schemas order</li>
 * <li>Specific schema-related order-by criteria</li>
 * <li>Rollup</li>
 * <li>Custom hash partitioning fields</li>
 * </ul>
 * 
 */
public class TupleMRConfig {

	private final static String CONF_PANGOOL_CONF = TupleMRConfig.class.getName()
	    + ".pangool.conf";

	static final JsonFactory FACTORY = new JsonFactory();
	static final ObjectMapper MAPPER = new ObjectMapper(FACTORY);

	static {
		FACTORY.enable(JsonParser.Feature.ALLOW_COMMENTS);
		FACTORY.setCodec(MAPPER);
	}

	private List<String> schemasNames = new ArrayList<String>();
	private Map<String, Integer> schemaNameToId = new HashMap<String, Integer>();
	private Map<String,Map<String,String>> schemaFieldAliases = new HashMap<String,Map<String,String>>();

	/**
	 * Common criteria specifies the order of the fields that are common among
	 * schemas and will be sorted before reaching the sourceOrder
	 */
	private Criteria commonCriteria;

	/**
	 * These criterias are used in {@link SortComparator} to sort specific fields
	 * from two tuples with the same schema, after the commonCriteria and
	 * schemaOrder
	 */
	private List<Criteria> specificCriterias = new ArrayList<Criteria>();

	/**
	 * Defines the order in which the different intermediate schemas will be
	 * sorted in {@link SortComparator}
	 * 
	 */
	private Order schemasOrder;

	/**
	 * Intermediate schemas
	 */
	private List<Schema> schemas = new ArrayList<Schema>();
	private List<String> groupByFields;
	private List<String> customPartitionFields = new ArrayList<String>();
	private String rollupFrom;

	private SerializationInfo serInfo;

	/**
	 * Returns a defined intermediate schema with the specified name
	 */
	public Schema getIntermediateSchema(String schemaName) {
		Integer id = getSchemaIdByName(schemaName);
		return (id == null) ? null : schemas.get(id);
	}

	/**
	 * Returns a defined intermediate schema with the specified schemaId.<br>
	 * The schemaId follows the order of schema definition in
	 * {@link TupleMRConfig#addIntermediateSchema(Schema)}
	 */
	public Schema getIntermediateSchema(int schemaId) {
		return schemas.get(schemaId);
	}

	/**
	 * Returns the schemaId from the schema's name.
	 */
	public Integer getSchemaIdByName(String name) {
		return schemaNameToId.get(name);
	}

	/**
	 * Returns a list with the names of all the intermediate schemas.
	 */
	public List<String> getIntermediateSchemaNames() {
		return schemasNames;
	}

	/**
	 * Returns all the intermediate schemas defined.
	 */
	public List<Schema> getIntermediateSchemas() {
		return schemas;
	}

	/**
	 * Returns the order that will be used to sort tuples with different schemas
	 * after being compared by commonOrder.
	 */
	public Order getSchemasOrder() {
		return schemasOrder;
	}

	/**
	 * Returns the order that will be used to sort tuples with different schemas
	 * after being compared by commonOrder and schemaOrder.
	 * 
	 */
	public List<Criteria> getSpecificOrderBys() {
		return specificCriterias;
	}

	/**
	 * Returns the custom fields used to partition tuples. By default if this list
	 * is null then the partition criteria used will match the groupByFields. In
	 * case of rollup then the fields used will be a subset of the groupByFields
	 * up to the rollupFrom field.
	 */
	public List<String> getCustomPartitionFields() {
		return customPartitionFields;
	}

	protected TupleMRConfig() {
	}

	/**
	 * Returns the {@link SerializationInfo} instance related to this
	 * configuration.
	 */
	public SerializationInfo getSerializationInfo() {
		if(serInfo == null) {
			try {
				this.serInfo = new SerializationInfo(this);
			} catch(Exception e) {
				throw new RuntimeException(e);
			}
		}
		return this.serInfo;
	}

	/**
	 * Returns the number of intermediate schemas defined
	 */
	public int getNumIntermediateSchemas() {
		return schemas.size();
	}

	/**
	 * Returns the criteria used to sort fields that are common among the
	 * intermediate schemas. This criteria is the first used in
	 * {@link SortComparator} and in {@link GroupComparator}
	 */
	public Criteria getCommonCriteria() {
		return commonCriteria;
	}

	/**
	 * Returns the fields that are common among all the intermediate schemas that
	 * will be used to group by the tuples emitted from the {@link TupleMapper}
	 */
	public List<String> getGroupByFields() {
		return groupByFields;
	}
	
	/**
	 * Returns a map that contains for every schema a list of field aliases. 
	 * Field aliases are needed to declare common fields to be used in 
	 * {@link #setGroupByFields(List)} and{@link #setGroupByFields(List)}
	 * 
	 */
	public Map<String,Map<String,String>> getSchemaFieldAliases(){
		return schemaFieldAliases;
	}
	
	public Map<String,String> getFieldAliases(String schemaName){
		return schemaFieldAliases.get(schemaName);
	}
	

	/**
	 * Returns the fields that are a subset from the groupBy fields and will be
	 * used when rollup is needed.
	 * 
	 * @see RollupReducer
	 */
	public List<String> calculateRollupBaseFields() {
		if(rollupFrom == null) {
			return getGroupByFields();
		}

		List<String> result = new ArrayList<String>();
		for(SortElement element : commonCriteria.getElements()) {
			result.add(element.getName());
			if(element.getName().equals(rollupFrom)) {
				break;
			}
		}
		return result;
	}

	/**
	 * Returns the field from which the rollup will be performed
	 */
	public String getRollupFrom() {
		return rollupFrom;
	}

	private void addIntermediateSchema(Schema schema) throws TupleMRException {
		if(schemasNames.contains(schema.getName())) {
			throw new TupleMRException("There's a schema with that name '" + schema.getName()
			    + "'");
		}
		schemaNameToId.put(schema.getName(), schemasNames.size());
		schemasNames.add(schema.getName());
		schemas.add(schema);
	}

	void setIntermediateSchemas(Collection<Schema> schemas) throws TupleMRException {
		for(Schema s : schemas) {
			addIntermediateSchema(s);
		}
	}

	void setGroupByFields(List<String> groupByFields) {
		this.groupByFields = groupByFields;
	}

	void setRollupFrom(String rollupFrom) throws TupleMRException {
		this.rollupFrom = rollupFrom;
	}

	void setCommonCriteria(Criteria ordering) {
		this.commonCriteria = ordering;
	}

	void setSourceOrder(Order order) {
		this.schemasOrder = order;
	}

	void setCustomPartitionFields(List<String> customPartitionFields) {
		this.customPartitionFields = customPartitionFields;
	}

	void setSecondarySortBy(String sourceName, Criteria criteria) throws TupleMRException {
		if(this.specificCriterias.isEmpty()) {
			initSecondaryCriteriasWithNull();
		}
		Integer pos = getSchemaIdByName(sourceName);
		specificCriterias.set(pos, criteria);
	}
	
	void setSchemaFieldAliases(Map<String,Map<String,String>> schemaAliases) throws TupleMRException {
		this.schemaFieldAliases = schemaAliases;
	}

	private void initSecondaryCriteriasWithNull() {
		List<Criteria> r = new ArrayList<Criteria>(schemas.size());
		for(int i = 0; i < schemas.size(); i++) {
			r.add(null);
		}
		this.specificCriterias = r;
	}

	public static TupleMRConfig get(Configuration conf) throws TupleMRException {
		String serialized = conf.get(TupleMRConfig.CONF_PANGOOL_CONF);
		if(serialized == null || serialized.isEmpty()) {
			return null;
		}
		try {
			TupleMRConfig mrConf = TupleMRConfig.parse(serialized);
			deserializeComparators(conf, mrConf);
			return mrConf;

		} catch(IOException e) {
			throw new TupleMRException(e);
		}
	}

	public static void set(TupleMRConfig mrConfig, Configuration conf)
	    throws TupleMRException {
		conf.set(CONF_PANGOOL_CONF, mrConfig.toString());
		serializeComparators(mrConfig, conf);
	}

	// Stores the instances and the references to the instances (common|field or
	// schemaId|field)
	public static final String CONF_COMPARATOR_REFERENCES = "pangool.comparator.references";
	public static final String CONF_COMPARATOR_INSTANCES = "pangool.comparator.instances";
	private static final String COMMON = "common";

	/**
	 * Serializes the custom compartors. It uses the distributed cache
	 * serialization.<br>
	 * Two config properties are used. The first for storing the reference. For
	 * example "common|address" refers to the sort comparator for address in the
	 * common order. "1|postalCode" refers to the sort comparator for the specific
	 * sort on field1 for the schema with schemaId = 1. The other config property
	 * stores the instance file paths where the instances are stored in the
	 * distributed cache.
	 */
	static void serializeComparators(TupleMRConfig tupleMRConfig, Configuration conf)
	    throws TupleMRException {
		List<String> comparatorRefs = new ArrayList<String>();
		List<String> comparatorInstanceFiles = new ArrayList<String>();

		// We use "common" as the prefix for the common criteria
		serializeComparators(tupleMRConfig.getCommonCriteria(), conf, comparatorRefs,
		    comparatorInstanceFiles, COMMON);

		List<Criteria> specificCriterias = tupleMRConfig.getSpecificOrderBys();
		// We use the schemaId as prefix for the specific sorting.
		for(int i = 0; i < specificCriterias.size(); i++) {
			serializeComparators(specificCriterias.get(i), conf, comparatorRefs,
			    comparatorInstanceFiles, i + "");
		}

		if(comparatorRefs.size() > 0) {
			conf.setStrings(CONF_COMPARATOR_REFERENCES, comparatorRefs.toArray(new String[] {}));
			conf.setStrings(CONF_COMPARATOR_INSTANCES,
			    comparatorInstanceFiles.toArray(new String[] {}));
		}
	}

	static void serializeComparators(Criteria criteria, Configuration conf,
	    List<String> comparatorRefs, List<String> comparatorInstanceFiles, String prefix)
	    throws TupleMRException {

		if(criteria == null) {
			return;
		}

		for(SortElement element : criteria.getElements()) {
			if(element.getCustomComparator() != null) {
				RawComparator<?> comparator = element.getCustomComparator();

				if(!(comparator instanceof Serializable)) {
					throw new TupleMRException("The class " + comparator.getClass()
					    + " is not Serializable."
					    + " The customs comparators must implement Serializable.");
				}

				String ref = prefix + "|" + element.getName();
				String uniqueName = UUID.randomUUID().toString() + '.' + "comparator.dat";
				try {
					DCUtils.serializeToDC(comparator, uniqueName, conf);
				} catch(Exception e) {
					throw new TupleMRException(e);
				}

				comparatorRefs.add(ref);
				comparatorInstanceFiles.add(uniqueName);
			}
		}
	}

	static void deserializeComparators(Configuration conf, TupleMRConfig mrConfig)
	    throws TupleMRException {
		String[] comparatorRefs = conf.getStrings(CONF_COMPARATOR_REFERENCES);
		String[] comparatorInstanceFiles = conf.getStrings(CONF_COMPARATOR_INSTANCES);

		if(comparatorRefs == null) {
			return;

		}
		try {
			for(int i = 0; i < comparatorRefs.length; i++) {
				String[] ref = comparatorRefs[i].split("\\|");
				String instanceFile = comparatorInstanceFiles[i];

				// Here we use "false" as last parameter because otherwise it could be
				// an infinite loop. We will call setConf() later.
				RawComparator<?> comparator = DCUtils.loadSerializedObjectInDC(conf,
				    RawComparator.class, instanceFile, false);

				if(ref[0].equals(COMMON)) {
					setComparator(mrConfig.getCommonCriteria(), ref[1], comparator);
				} else {
					setComparator(mrConfig.getSpecificOrderBys().get(new Integer(ref[0])), ref[1],
					    comparator);
				}
			}
		} catch(IOException e) {
			throw new TupleMRException(e);
		}
	}

	static void setComparator(Criteria criteria, String field, RawComparator<?> comparator) {
		for(SortElement element : criteria.getElements()) {
			if(element.getName().equals(field)) {
				element.setCustomComparator(comparator);
			}
		}
	}

	static TupleMRConfig parse(JsonNode node) throws IOException {
		try {
			TupleMRConfig result = new TupleMRConfig();
			Iterator<JsonNode> sources = node.get("sourceSchemas").getElements();
			while(sources.hasNext()) {
				JsonNode sourceNode = sources.next();
				result.addIntermediateSchema(Schema.parse(sourceNode));
			}
			Iterator<String> schemaNames = node.get("fieldAliases").getFieldNames();
			while(schemaNames.hasNext()){
				String schemaName = schemaNames.next();
				JsonNode aliasNode = node.get("fieldAliases").get(schemaName);
				Aliases aliases = Aliases.parse(aliasNode);
				result.schemaFieldAliases.put(schemaName,aliases.getAliases());
			}
			
			result.schemaFieldAliases = Collections.unmodifiableMap(result.schemaFieldAliases);
			
			Iterator<JsonNode> groupFieldsNode = node.get("groupByFields").getElements();
			List<String> groupFields = new ArrayList<String>();
			while(groupFieldsNode.hasNext()) {
				groupFields.add(groupFieldsNode.next().getTextValue());
			}
			result.groupByFields = Collections.unmodifiableList(groupFields);

			if(node.get("rollupFrom") != null) {
				result.rollupFrom = node.get("rollupFrom").getTextValue();
			}

			if(node.get("customPartitionFields") != null) {
				Iterator<JsonNode> partitionNodes = node.get("customPartitionFields")
				    .getElements();
				List<String> partitionFields = new ArrayList<String>();
				while(partitionNodes.hasNext()) {
					partitionFields.add(partitionNodes.next().getTextValue());
				}
				result.customPartitionFields = partitionFields;
			}

			JsonNode commonSortByNode = node.get("commonOrderBy");
			result.commonCriteria = Criteria.parse(commonSortByNode);
			result.schemasOrder = Order.valueOf(node.get("schemasOrder").getTextValue());

			Iterator<JsonNode> specificNode = node.get("specificOrderBys").getElements();
			result.specificCriterias = new ArrayList<Criteria>();
			while(specificNode.hasNext()) {
				JsonNode n = specificNode.next();
				result.specificCriterias.add(n.isNull() ? null : Criteria.parse(n));
			}

			return result;
		} catch(TupleMRException e) {
			throw new IOException(e);
		}
	}

//	private writeMap(Map<String,String> map, JsonGenerator gen){
//		
//		
//	}
	
	
	void toJson(JsonGenerator gen) throws IOException {
		gen.writeStartObject();
		gen.writeArrayFieldStart("sourceSchemas");
		for(Schema schema : schemas) {
			schema.toJson(gen);
		}
		gen.writeEndArray();
		gen.writeObjectFieldStart("fieldAliases");
		for (Map.Entry<String,Map<String,String>> entry : schemaFieldAliases.entrySet()){
			String schemaName = entry.getKey();
			Map<String,String> aliases = entry.getValue();
			gen.writeObjectField(schemaName, aliases);
		}
		gen.writeEndObject();
		
		gen.writeArrayFieldStart("groupByFields");
		for(String field : groupByFields) {
			gen.writeString(field);
		}
		gen.writeEndArray();

		if(customPartitionFields != null && !customPartitionFields.isEmpty()) {
			gen.writeArrayFieldStart("customPartitionFields");
			for(String field : customPartitionFields) {
				gen.writeString(field);
			}
			gen.writeEndArray();
		}

		if(rollupFrom != null) {
			gen.writeFieldName("rollupFrom");
			gen.writeString(rollupFrom);
		}

		gen.writeFieldName("commonOrderBy");
		commonCriteria.toJson(gen);

		gen.writeStringField("schemasOrder", schemasOrder.toString());

		// TODO this code should write a map with sourceName
		if(specificCriterias == null || specificCriterias.isEmpty()) {
			initSecondaryCriteriasWithNull();
		}
		gen.writeArrayFieldStart("specificOrderBys");
		for(Criteria c : specificCriterias) {
			if(c == null) {
				gen.writeNull();
			} else {
				c.toJson(gen);
			}
		}
		gen.writeEndArray();
		gen.writeEndObject();
	}

	@Override
	public String toString() {
		// TODO not use toJson as toString()... it is not complete.
		// Custom comparators does not appears here.
		return toJson(true);
	}

	protected String toJson(boolean pretty) {
		try {
			StringWriter writer = new StringWriter();
			JsonGenerator gen = FACTORY.createJsonGenerator(writer);
			if(pretty)
				gen.useDefaultPrettyPrinter();
			toJson(gen);
			gen.flush();
			return writer.toString();
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Parse a schema from the provided string. If named, the schema is added to
	 * the names known to this parser.
	 */
	public static TupleMRConfig parse(String s) throws IOException {
		return parse(FACTORY.createJsonParser(new StringReader(s)));
	}

	private static TupleMRConfig parse(JsonParser parser) throws IOException {
		try {
			return parse(MAPPER.readTree(parser));
		} catch(JsonParseException e) {
			throw new IOException(e);
		}
	}

	public boolean equals(Object a) {
		if(!(a instanceof TupleMRConfig)) {
			return false;
		}
		TupleMRConfig that = (TupleMRConfig) a;

		boolean e = this.getSchemasOrder() == that.getSchemasOrder()
		    && this.getCommonCriteria().equals(that.getCommonCriteria())
		    && this.getGroupByFields().equals(that.getGroupByFields())
		    && this.getIntermediateSchemas().equals(that.getIntermediateSchemas())
		    && this.getSpecificOrderBys().equals(that.getSpecificOrderBys())
		    && this.getSchemaFieldAliases().equals(that.getSchemaFieldAliases());
		    
		if(e) {
			if(this.getCustomPartitionFields() == null) {
				return that.getCustomPartitionFields() == null;
			} else {
				return this.getCustomPartitionFields().equals(that.getCustomPartitionFields());
			}
		} else {
			return false;
		}
	}
	
	@Override
	public int hashCode() {
	  assert false : "hashCode not designed";
  	return 42; // any arbitrary constant will do 
	}
}

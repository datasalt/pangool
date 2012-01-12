package com.datasalt.pangolin.pangool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.datasalt.pangolin.pangool.Schema.Field;
import com.datasalt.pangolin.pangool.SortCriteria.SortElement;

public class PangoolConfig {

	private final static String CONF_PANGOOL_CONF = PangoolConfig.class.getName() + ".pangool.conf";

	private Sorting sorting;
	private Map<Integer, Schema> schemes; // key is schema Id
	private List<String> groupByFields;
	private String rollupFrom;
	
	// --- Following properties are calculated after package-access build() is called --- //
	private Schema commonOrderedSchema;
	private Map<Integer, Schema> particularPartialOrderedSchemas;

	PangoolConfig() {
		schemes = new HashMap<Integer, Schema>();
	}

	void setSorting(Sorting sorting) {
		this.sorting = sorting;
	}

	Sorting getSorting() {
  	return sorting;
  }

	public List<String> getGroupByFields() {
  	return groupByFields;
  }

	public String getRollupFrom() {
  	return rollupFrom;
  }

	void addSchema(Integer schemaId, Schema schema) {
		schemes.put(schemaId, schema);
	}

	void setGroupByFields(String... groupByFields) {
		this.groupByFields = Collections.unmodifiableList(Arrays.asList(groupByFields));
	}

	void setRollupFrom(String rollupFrom) {
		this.rollupFrom = rollupFrom;
	}

	public Map<Integer, Schema> getSchemes() {
		return schemes;
	}
	
	public static void setPangoolConfig(PangoolConfig config, Configuration conf) throws JsonGenerationException, JsonMappingException, IOException {
		ObjectMapper jsonSerDe = new ObjectMapper();
		conf.set(CONF_PANGOOL_CONF, config.toStringAsJSON(jsonSerDe));
	}

	public static PangoolConfig get(Configuration conf) throws JsonParseException, JsonMappingException, IOException, CoGrouperException {
		ObjectMapper jsonSerDe = new ObjectMapper();
		return fromJSON(conf.get(CONF_PANGOOL_CONF), jsonSerDe);
	}
	
	// ------------------------------------------------------------- //
	
	/**
	 * Package-access build() called to finally calculate things like the common schema
	 * and perform sanity validation
	 */
	void build() throws CoGrouperException {
		
		/*
		 * Calculate common Fields
		 */
		List<Field> commonFields = new ArrayList<Field>();
		
		Collection<Schema> schemas = getSchemes().values();
		int count = 0;
		// Calculate the common fields between all schemas
		for(Schema schema : schemas) {
			if(count == 0) {
				// First schema has all common fields
				commonFields.addAll(schema.getFields());
			} else {
				// The rest of schemas are tested against the (so far) common fields
				Iterator<Field> iterator = commonFields.iterator();
				while(iterator.hasNext()) {
					Field field = iterator.next();
					if(!schema.containsFieldName(field.getName())) {
						iterator.remove();
					}
				}
			}
			count++;
		}

		checkSortCriteriaSanity(commonFields);
		
		/*
		 * Calculate common ordered schema
		 */
		List<Field> commonSchema = new ArrayList<Field>();
		
		for(SortElement sortElement: getSorting().getSortCriteria().getSortElements()) {
			if(sortElement.getFieldName().equals(Field.SOURCE_ID_FIELD)) {
				commonSchema.add(Field.SOURCE_ID);
				continue;
			}
			for(Field field: commonFields) {
				if(sortElement.getFieldName().equals(field.getName())) {
					commonSchema.add(field);
				}
			}
		}
		
		commonOrderedSchema = new Schema(commonSchema);

		/*
		 * Calculate the particular partial ordered schemas
		 */
		particularPartialOrderedSchemas = new HashMap<Integer, Schema>();
		for(Map.Entry<Integer, Schema> schema: getSchemes().entrySet()) {
			
			List<Field> particularSchema = new ArrayList<Field>();
			
			// Check particular sort order for schema
			
			SortCriteria particularSorting = getSorting().getSecondarySortCriterias().get(schema.getKey());
			if(particularSorting != null) {
				for(SortElement element: particularSorting.getSortElements()) {
					particularSchema.add(schema.getValue().getField(element.getFieldName()));
				}
			}
			
			// Find the fields that are not common (and that are not included in the particular sort)
			List<Field> nonCommonFields = new ArrayList<Field>();
			for(Field field: schema.getValue().getFields()) {
				if(!commonOrderedSchema.containsFieldName(field.getName()) && !particularSchema.contains(field)) {
					nonCommonFields.add(field);
				}
			}
			// Sort them alphabetically
			Collections.sort(nonCommonFields, new Comparator<Field>() {
				@Override
        public int compare(Field field1, Field field2) {
	        return field1.getName().compareTo(field2.getName());
        }
			});
			
			// Create the ordered schema
			particularSchema.addAll(nonCommonFields);
			particularPartialOrderedSchemas.put(schema.getKey(), new Schema(nonCommonFields));
		}		
	}
	
	/**
	 * Performs SortCriteria validation
	 * 
	 * @param commonFieldNames
	 * @throws CoGrouperException
	 */
	private void checkSortCriteriaSanity(List<Field> commonFields) throws CoGrouperException {
		List<String> commonFieldNames = new ArrayList<String>();
		for(Field field: commonFields) {
			commonFieldNames.add(field.getName());
		}
		
		// Check that sortCriteria is a combination of (common) fields from schema

		for(SortElement sortElement : getSorting().getSortCriteria().getSortElements()) {
			if(sortElement.getFieldName().equals(Field.SOURCE_ID_FIELD)) {
				continue;
			}
			if(!commonFieldNames.contains(sortElement.getFieldName())) {
				String extraReason = getSchemes().values().size() > 1 ? " common " : "";
				throw new CoGrouperException("Sort element [" + sortElement.getFieldName() + "] not contained in "
				    + extraReason + " Schema fields: " + commonFieldNames);
			}
		}

		for(Map.Entry<Integer, SortCriteria> secondarySortCriteria : getSorting().getSecondarySortCriterias()
		    .entrySet()) {

			// Check that each particular sort criteria (not common fields) matches existent fields for the schema
			
			int schemaId = secondarySortCriteria.getKey();
			Schema schema = getSchemes().get(schemaId);
			if(schema == null) {
				throw new CoGrouperException("Sort criteria for unexisting schema [" + schemaId + "]");
			}
			for(SortElement sortElement : secondarySortCriteria.getValue().getSortElements()) {
				if(!schema.containsFieldName(sortElement.getFieldName())) {
					throw new CoGrouperException("Particular secondary sort for schema [" + schemaId
					    + "] has non-existent field [" + sortElement.getFieldName() + "]");
				}
			}
		}

		// Check that group by fields is a prefix of common sort criteria
		
		SortCriteria sortCriteria = getSorting().getSortCriteria();
		for(int i = 0; i < getGroupByFields().size(); i++) {
			if(!sortCriteria.getSortElements()[i].getFieldName().equals(getGroupByFields().get(i))) {
				throw new CoGrouperException("Group by fields " + getGroupByFields()
				    + " is not a prefix of sort criteria: " + sortCriteria);
			}
		}
	}

	/**
	 * Returns a sorted schema that represents the sorted fields common to all schemas
	 * in the order they can be serialized or compared.
	 *  
	 * @param sortCriteria
	 * @return
	 */
	public Schema getCommonOrderedSchema() {
		return commonOrderedSchema;
	}
	
	/**
	 * Returns a map of ordered schemas that represent the way each non-common part of
	 * the schemas can be serialized or compared.
	 * 
	 * @param sortCriteria
	 * @return
	 */
	public Map<Integer, Schema> getParticularPartialOrderedSchemas() {
		return particularPartialOrderedSchemas;
	}
	
	public String toString() {
		StringBuilder b = new StringBuilder();
		b.append("sorting: ").append(sorting.toString()).append(" ");
		b.append("schemes: ").append(schemes.toString()).append(" ");
		b.append("groupByFields: ").append(groupByFields).append(" ");
		b.append("rollupFrom: ").append(rollupFrom);
		return b.toString();
	}

	public String toStringAsJSON(ObjectMapper mapper) throws JsonGenerationException, JsonMappingException, IOException {
		Map<String, Object> jsonableData = new HashMap<String, Object>();
		jsonableData.put("sorting", sorting.toStringAsJSON(mapper));
		Map<String, String> jsonableSchemes = new HashMap<String, String>();
		
		for(Map.Entry<Integer, Schema> schemaEntry : schemes.entrySet()) {
			jsonableSchemes.put(schemaEntry.getKey() + "", schemaEntry.getValue().toString());
		}
		
		jsonableData.put("schemes", jsonableSchemes);
		jsonableData.put("groupByFields", groupByFields);
		jsonableData.put("rollupFrom", rollupFrom);
		return mapper.writeValueAsString(jsonableData);
	}
	
	@SuppressWarnings("unchecked")
  static PangoolConfig fromJSON(String json, ObjectMapper mapper) throws JsonParseException, JsonMappingException, IOException, CoGrouperException {
		PangoolConfig config = new PangoolConfig();
		HashMap<String, Object> jsonData = mapper.readValue(json, HashMap.class);
		
		config.setRollupFrom((String) jsonData.get("rollupFrom"));
		
		ArrayList<String> list = (ArrayList<String>) jsonData.get("groupByFields");
		config.setGroupByFields(list.toArray(new String[0]));
		
		Map<String, String> jsonSchemes = (Map<String, String>) jsonData.get("schemes");
		
		for(Map.Entry<String, String> jsonScheme: jsonSchemes.entrySet()) {
			config.addSchema(Integer.parseInt(jsonScheme.getKey()), Schema.parse(jsonScheme.getValue()));
		}
		
		config.setSorting(Sorting.fromJSON((String) jsonData.get("sorting"), mapper));
		config.build();
		
		return config;
	}
}

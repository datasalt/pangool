package com.datasalt.pangolin.pangool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.datasalt.pangolin.pangool.Schema.Field;
import com.datasalt.pangolin.pangool.SortCriteria.SortElement;

/**
 * 
 * @author pere
 *
 */
@SuppressWarnings("rawtypes")
public class PangoolConfigBuilder {

	PangoolConfig config = new PangoolConfig();
	
	public void setSorting(Sorting sorting) {
		config.setSorting( sorting );
	}

	public void setCustomPartitionerFields(String... customPartitionerFields) {
  	config.setCustomPartitionerFields(customPartitionerFields);
  }

	public void addSchema(Integer schemaId, Schema schema) throws CoGrouperException {
		if(config.getSchemes().containsKey(schemaId)) {
			throw new CoGrouperException("Schema already present: " + schemaId);
		}

		if(schema == null) {
			throw new CoGrouperException("Schema may not be null");
		}

		config.addSchema(schemaId, schema);
	}

	public void setGroupByFields(String... groupByFields) {
		config.setGroupByFields(groupByFields);
	}
	
	public void setRollupFrom(String rollupFrom) {
		config.setRollupFrom(rollupFrom);
	}

	private void raiseExceptionIfNull(Object ob, String message) throws CoGrouperException {
		if(ob == null) {
			throw new CoGrouperException(message);
		}
	}

  private void raiseExceptionIfEmpty(Collection ob, String message) throws CoGrouperException {
		if(ob == null || ob.isEmpty()) {
			throw new CoGrouperException(message);
		}
	}

	/**
	 * build() called to finally calculate things like the common schema
	 * and perform sanity validation
	 */
	public PangoolConfig build() throws CoGrouperException {
		
		raiseExceptionIfEmpty(config.getSchemes().values(), "Need to set at least one schema");
		raiseExceptionIfNull(config.getSorting(), "Need to set sorting");
		raiseExceptionIfNull(config.getSorting().getSortCriteria(), "Need to set sorting criteria");
		raiseExceptionIfNull(config.getGroupByFields(), "Need to set fields to group by");

		/*
		 * Calculate common Fields
		 */
		List<Field> commonFields = new ArrayList<Field>();
		
		Collection<Schema> schemas = config.getSchemes().values();
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
		
		for(SortElement sortElement: config.getSorting().getSortCriteria().getSortElements()) {
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
		
		Schema commonOrderedSchema = new Schema(commonSchema);
		config.setCommonOrderedSchema(commonOrderedSchema);
		
		/*
		 * Calculate the particular partial ordered schemas
		 */
		Map<Integer, Schema> particularPartialOrderedSchemas = new HashMap<Integer, Schema>();
		for(Map.Entry<Integer, Schema> schema: config.getSchemes().entrySet()) {
			
			List<Field> particularSchema = new ArrayList<Field>();
			
			// Check particular sort order for schema
			
			SortCriteria particularSorting = config.getSorting().getSecondarySortCriterias().get(schema.getKey());
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
			
			config.setParticularPartialOrderedSchemas(particularPartialOrderedSchemas);
		}		
		
		return config;
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

		for(SortElement sortElement : config.getSorting().getSortCriteria().getSortElements()) {
			if(sortElement.getFieldName().equals(Field.SOURCE_ID_FIELD)) {
				continue;
			}
			if(!commonFieldNames.contains(sortElement.getFieldName())) {
				String extraReason = config.getSchemes().values().size() > 1 ? " common " : "";
				throw new CoGrouperException("Sort element [" + sortElement.getFieldName() + "] not contained in "
				    + extraReason + " Schema fields: " + commonFieldNames);
			}
		}

		for(Map.Entry<Integer, SortCriteria> secondarySortCriteria : config.getSorting().getSecondarySortCriterias()
		    .entrySet()) {

			// Check that each particular sort criteria (not common fields) matches existent fields for the schema
			
			int schemaId = secondarySortCriteria.getKey();
			Schema schema = config.getSchemes().get(schemaId);
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
		
		SortCriteria sortCriteria = config.getSorting().getSortCriteria();
		for(int i = 0; i < config.getGroupByFields().size(); i++) {
			if(!sortCriteria.getSortElements()[i].getFieldName().equals(config.getGroupByFields().get(i))) {
				throw new CoGrouperException("Group by fields " + config.getGroupByFields()
				    + " is not a prefix of sort criteria: " + sortCriteria);
			}
		}
	}
	
  public static PangoolConfig fromJSON(String json, ObjectMapper mapper) throws JsonParseException, JsonMappingException, IOException, CoGrouperException {
		PangoolConfigBuilder configBuilder = new PangoolConfigBuilder();
		configBuilder.config.fromJSON(json, mapper);
		return configBuilder.build();
	}
	
	public static PangoolConfig get(Configuration conf) throws JsonParseException, JsonMappingException, IOException, CoGrouperException {
		ObjectMapper jsonSerDe = new ObjectMapper();
		return fromJSON(conf.get(PangoolConfig.CONF_PANGOOL_CONF), jsonSerDe);
	}
}

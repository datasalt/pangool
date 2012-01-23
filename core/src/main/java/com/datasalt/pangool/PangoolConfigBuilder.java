package com.datasalt.pangool;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.datasalt.pangool.io.tuple.ITuple.InvalidFieldException;
import com.datasalt.pangool.Schema.Field;
import com.datasalt.pangool.Schema.Fields;
import com.datasalt.pangool.SortCriteria.SortElement;

/**
 * 
 * @author pere
 *
 */
@SuppressWarnings("rawtypes")
public class PangoolConfigBuilder {

	private PangoolConfig config = new PangoolConfig();
	
	public static PangoolConfigBuilder newOne() {
		return new PangoolConfigBuilder();
	}
	
	public PangoolConfigBuilder setSorting(Sorting sorting) {
		config.setSorting( sorting );
		return this;
	}

	public PangoolConfigBuilder addSchema(Integer schemaId, Schema schema) throws CoGrouperException {
		if(config.getSchemes().containsKey(schemaId)) {
			throw new CoGrouperException("Schema already present: " + schemaId);
		}

		if(schema == null) {
			throw new CoGrouperException("Schema may not be null");
		}

		config.addSchema(schemaId, schema);
		return this;
	}

	public PangoolConfigBuilder setGroupByFields(String... groupByFields) {
		config.setGroupByFields(groupByFields);
		return this;
	}
	
	public PangoolConfigBuilder setRollupFrom(String rollupFrom) {
		config.setRollupFrom(rollupFrom);
		return this;
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
		Fields commonFields = new Fields(); // same name, same type
		
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
					if(schema.containsFieldName(field.getName())) {
						Field fieldInSchema = schema.getField(field.getName());
						if(fieldInSchema.getType().equals(field.getType())) {
							continue; // same name, same type -> field remains
						}
					}
					iterator.remove(); // otherwise it is discarded
				}
			}
			count++;
		}

		checkSortCriteriaSanity(commonFields);
		
		/*
		 * Calculate common ordered schema
		 */
		Fields commonSchema = new Fields();
		
		for(SortElement sortElement: config.getSorting().getSortCriteria().getSortElements()) {
			if(sortElement.getFieldName().equals(Field.SOURCE_ID_FIELD_NAME)) {
				commonSchema.add(Field.SOURCE_ID);
				continue;
			}
			commonSchema.add(commonFields.get(sortElement.getFieldName()));
		}
		
		// Add sourceId to the end if it hasn't been specified
		if(!config.getSorting().isSourceIdFieldContained() && config.getSchemes().values().size() > 1) {
			commonSchema.add(Field.SOURCE_ID);
		}
		
		Schema commonOrderedSchema = new Schema(commonSchema);
		config.setCommonOrderedSchema(commonOrderedSchema);
		
		/*
		 * Calculate the specific partial ordered schemas
		 */
		Map<Integer, Schema> specificOrderedSchemas = new HashMap<Integer, Schema>();
		for(Map.Entry<Integer, Schema> schema: config.getSchemes().entrySet()) {
			
			Fields specificSchema = new Fields();
			
			// Check specific sort order for schema -- add these fields first
			SortCriteria specificSorting = config.getSorting().getSpecificSortCriterias().get(schema.getKey());
			if(specificSorting != null) {
				for(SortElement element: specificSorting.getSortElements()) {
					specificSchema.add(schema.getValue().getField(element.getFieldName()));
				}
			}
			
			// Find the fields that are not present in the common sorting
			Fields nonCommonFields = new Fields();
			for(Field field: schema.getValue().getFields()) {
				if(!commonOrderedSchema.containsFieldName(field.getName()) && !specificSchema.contains(field.getName())) {
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
			specificSchema.addAll(nonCommonFields);
			specificOrderedSchemas.put(schema.getKey(), new Schema(specificSchema));
			
			config.setSpecificOrderedSchemas(specificOrderedSchemas);
		}		
		
		return config;
	}
	
	/**
	 * Performs SortCriteria validation
	 * 
	 * @param commonFieldNames
	 * @throws CoGrouperException
	 */
	private void checkSortCriteriaSanity(Fields commonFields) throws CoGrouperException {

		// Sorting by SourceId is not allowed if we only have one schema
		if(config.getSorting().isSourceIdFieldContained() && config.getSchemes().values().size() == 1) {
			throw new CoGrouperException("Invalid " + Field.SOURCE_ID_FIELD_NAME + " field for single schema sorting. " + Field.SOURCE_ID_FIELD_NAME + " may only be added when sorting more than one schema.");
		}
		
		// Check that sortCriteria is a combination of (common) fields from schema
		for(SortElement sortElement : config.getSorting().getSortCriteria().getSortElements()) {
			if(sortElement.getFieldName().equals(Field.SOURCE_ID_FIELD_NAME)) {
				continue;
			}
			if(!commonFields.contains(sortElement.getFieldName())) {
				String extraReason = config.getSchemes().values().size() > 1 ? " common " : "";
				throw new CoGrouperException("Sort element [" + sortElement.getFieldName() + "] not contained in "
				    + extraReason + " Schema fields: " + commonFields);
			}
		}

		for(Map.Entry<Integer, SortCriteria> secondarySortCriteria : config.getSorting().getSpecificSortCriterias()
		    .entrySet()) {

			// Check that each specific sort criteria matches existent fields for the schema
			int schemaId = secondarySortCriteria.getKey();
			Schema schema = config.getSchemes().get(schemaId);
			if(schema == null) {
				throw new CoGrouperException("Sort criteria for unexisting schema [" + schemaId + "]");
			}
			for(SortElement sortElement : secondarySortCriteria.getValue().getSortElements()) {
				if(!schema.containsFieldName(sortElement.getFieldName())) {
					throw new CoGrouperException("Specific secondary sort for schema [" + schemaId
					    + "] has non-existent field [" + sortElement.getFieldName() + "]");
				}
				// We also check that specific sorting fields are not included in common sorting
				if(config.getSorting().getSortCriteria().getSortElementByFieldName(sortElement.getFieldName()) != null) {
					throw new CoGrouperException("Specific secondary sort for schema [" + schemaId
					    + "] has already added field to common sorting [" + sortElement.getFieldName() + "]");					
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
	
  public static PangoolConfig fromJSON(String json, ObjectMapper mapper) throws JsonParseException, JsonMappingException, IOException, CoGrouperException, NumberFormatException, InvalidFieldException {
		PangoolConfigBuilder configBuilder = new PangoolConfigBuilder();
		configBuilder.config.fromJSON(json, mapper);
		return configBuilder.build();
	}
	
	public static PangoolConfig get(Configuration conf) throws CoGrouperException {
		ObjectMapper jsonSerDe = new ObjectMapper();
		try {
	    return fromJSON(conf.get(PangoolConfig.CONF_PANGOOL_CONF), jsonSerDe);
    } catch (JsonParseException e) {
    	fail(e);
    } catch(JsonMappingException e) {
    	fail(e);
    } catch(NumberFormatException e) {
    	fail(e);
    } catch(IOException e) {
    	fail(e);
    }
		return null;
	}
	
	private static void fail(Exception e) throws CoGrouperException {
		throw new CoGrouperException(e);
	}
}

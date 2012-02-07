package com.datasalt.pangool;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.datasalt.pangool.Schema.Field;
import com.datasalt.pangool.SortCriteria.SortElement;
import com.datasalt.pangool.io.tuple.ITuple.InvalidFieldException;


@SuppressWarnings("rawtypes")
public class CoGrouperConfigBuilder {

	private CoGrouperConfig config = new CoGrouperConfig();
	
	public static CoGrouperConfigBuilder newOne() {
		return new CoGrouperConfigBuilder();
	}
	
	public CoGrouperConfigBuilder setSorting(Sorting sorting) {
		config.setSorting( sorting );
		return this;
	}

	public CoGrouperConfigBuilder addSchema(int schemaId, Schema schema) throws CoGrouperException {
		validateSchema(schemaId, schema);
		config.addSchema(schemaId, schema);
		return this;
	}

	private void validateSchema(Integer schemaId, Schema schema) throws CoGrouperException {
		if(config.getSchemas().containsKey(schemaId)) {
			throw new CoGrouperException("Schema already present: " + schemaId);
		}

		if(schema == null) {
			throw new CoGrouperException("Schema must not be null");
		}
	}
	
	public CoGrouperConfigBuilder setGroupByFields(String... groupByFields) {
		config.setGroupByFields(groupByFields);
		return this;
	}
	
	public CoGrouperConfigBuilder setRollupFrom(String rollupFrom) {
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
	public CoGrouperConfig build() throws CoGrouperException {
		
		raiseExceptionIfEmpty(config.getSchemas().values(), "Need to set at least one schema");
		raiseExceptionIfNull(config.getSorting(), "Need to set sorting");
		raiseExceptionIfNull(config.getSorting().getCommonSortCriteria(), "Need to set sorting criteria");
		raiseExceptionIfNull(config.getGroupByFields(), "Need to set fields to group by");
		return config;
	}
	
//	/**
//	 * Performs SortCriteria validation
//	 * 
//	 * @param commonFieldNames
//	 * @throws CoGrouperException
//	 */
//	private void checkSortCriteriaSanity(Fields commonFields) throws CoGrouperException {
//
//		// Sorting by SourceId is not allowed if we only have one schema
//		if(config.getSorting().isSourceIdFieldContained() && config.getSchemas().values().size() == 1) {
//			throw new CoGrouperException("Invalid " + Field.SOURCE_ID_FIELD_NAME + " field for single schema sorting. " + Field.SOURCE_ID_FIELD_NAME + " may only be added when sorting more than one schema.");
//		}
//		
//		// Check that sortCriteria is a combination of (common) fields from schema
//		for(SortElement sortElement : config.getSorting().getCommonSortCriteria().getSortElements()) {
//			if(sortElement.getFieldName().equals(Field.SOURCE_ID_FIELD_NAME)) {
//				continue;
//			}
//			if(!commonFields.contains(sortElement.getFieldName())) {
//				String extraReason = config.getSchemas().values().size() > 1 ? " common " : "";
//				throw new CoGrouperException("Sort element [" + sortElement.getFieldName() + "] not contained in "
//				    + extraReason + " Schema fields: " + commonFields);
//			}
//		}
//
//		for(Map.Entry<Integer, SortCriteria> secondarySortCriteria : config.getSorting().getSpecificSortCriterias()
//		    .entrySet()) {
//
//			// Check that each specific sort criteria matches existent fields for the schema
//			int schemaId = secondarySortCriteria.getKey();
//			Schema schema = config.getSchemas().get(schemaId);
//			if(schema == null) {
//				throw new CoGrouperException("Sort criteria for unexisting schema [" + schemaId + "]");
//			}
//			for(SortElement sortElement : secondarySortCriteria.getValue().getSortElements()) {
//				if(!schema.containsFieldName(sortElement.getFieldName())) {
//					throw new CoGrouperException("Specific secondary sort for schema [" + schemaId
//					    + "] has non-existent field [" + sortElement.getFieldName() + "]");
//				}
//				// We also check that specific sorting fields are not included in common sorting
//				for(SortElement element: config.getSorting().getCommonSortCriteria().getSortElements()) {
//					if(element.getFieldName().equals(sortElement.getFieldName())) {
//						throw new CoGrouperException("Specific secondary sort for schema [" + schemaId
//						    + "] has already added field to common sorting [" + sortElement.getFieldName() + "]");											
//					}
//				}
//			}
//		}
//
//		// Check that group by fields is a prefix of common sort criteria
//		SortCriteria sortCriteria = config.getSorting().getCommonSortCriteria();
//		for(int i = 0; i < config.getGroupByFields().size(); i++) {
//			if(!sortCriteria.getSortElements()[i].getFieldName().equals(config.getGroupByFields().get(i))) {
//				throw new CoGrouperException("Group by fields " + config.getGroupByFields()
//				    + " is not a prefix of sort criteria: " + sortCriteria);
//			}
//		}
//	}
	
  public static CoGrouperConfig fromJSON(String json, ObjectMapper mapper) throws JsonParseException, JsonMappingException, IOException, CoGrouperException, NumberFormatException, InvalidFieldException {
		CoGrouperConfigBuilder configBuilder = new CoGrouperConfigBuilder();
		configBuilder.config.fromJSON(json, mapper);
		return configBuilder.build();
	}
	
	public static CoGrouperConfig get(Configuration conf) throws CoGrouperException {
		ObjectMapper jsonSerDe = new ObjectMapper();
		try {
			String serialized =conf.get(CoGrouperConfig.CONF_PANGOOL_CONF);
	    return (serialized == null || serialized.isEmpty()) ? null : fromJSON(serialized, jsonSerDe);
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

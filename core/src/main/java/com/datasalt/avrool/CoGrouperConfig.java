package com.datasalt.avrool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.datasalt.avrool.Schema.Field;
import com.datasalt.avrool.io.tuple.ITuple;
import com.datasalt.avrool.io.tuple.ITuple.InvalidFieldException;

/**
 * 
 * @author pere
 *
 */
public class CoGrouperConfig {

	final static String CONF_PANGOOL_CONF = CoGrouperConfig.class.getName() + ".pangool.conf";

	private Sorting sorting;
	private Map<Integer, Schema> schemes; // key is schema Id
	private List<String> groupByFields;
	private String rollupFrom;
	
	private Schema commonOrderedSchema;
	private Map<Integer, Schema> specificOrderedSchemas;

	CoGrouperConfig() {
		schemes = new HashMap<Integer, Schema>();
	}

	void setSorting(Sorting sorting) {
		this.sorting = sorting;
	}

	public Sorting getSorting() {
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
	
	public Schema getSchemaBySourceId(int sourceId){
		return schemes.get(sourceId);
	}
	
	public static void setPangoolConfig(CoGrouperConfig config, Configuration conf) throws JsonGenerationException, JsonMappingException, IOException {
		ObjectMapper jsonSerDe = new ObjectMapper();
		conf.set(CONF_PANGOOL_CONF, config.toStringAsJSON(jsonSerDe));
	}

	// ------------------------------------------------------------- //

	void setCommonOrderedSchema(Schema commonOrderedSchema) {
  	this.commonOrderedSchema = commonOrderedSchema;
  }

	void setSpecificOrderedSchemas(Map<Integer, Schema> specificOrderedSchemas) {
  	this.specificOrderedSchemas = specificOrderedSchemas;
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
	public Map<Integer, Schema> getSpecificOrderedSchemas() {
		return specificOrderedSchemas;
	}
	
	public Schema getSpecificOrderedSchema(int sourceId){
		return specificOrderedSchemas.get(sourceId);
	}
	
	/**
	 * Get the schema of this tuple.
	 * 
	 * @param tuple
	 */
	public Schema getSchema(ITuple tuple) {
		return getSchemes().get(getSourceId(tuple));
	}
	
	/**
	 * Get the source Id for this tuple
	 * 
	 * @param tuple
	 * @return
	 */
	public int getSourceId(ITuple tuple) {
		Integer schemeId = tuple.getInt(Field.SOURCE_ID_FIELD_NAME);
		if(schemeId == null) { // single Schema config
			return getSchemes().entrySet().iterator().next().getKey();
		} else { 
			return schemeId;
		}
		
	}
	
	public String toString() {
		StringBuilder b = new StringBuilder();
		b.append("sorting: ").append(sorting.toString()).append(" ");
		b.append("schemes: ").append(schemes.toString()).append(" ");
		b.append("groupByFields: ").append(groupByFields).append(" ");
		b.append("rollupFrom: ").append(rollupFrom);
		return b.toString();
	}

	@SuppressWarnings("unchecked")
	void fromJSON(String json, ObjectMapper mapper) throws CoGrouperException {
		
		if (json == null){
			throw new CoGrouperException("Non existing pangool config set in Configuration");
		}
		
		try{
			HashMap<String, Object> jsonData = mapper.readValue(json, HashMap.class);
			
			setRollupFrom((String) jsonData.get("rollupFrom"));
			
			ArrayList<String> list = (ArrayList<String>) jsonData.get("groupByFields");
			setGroupByFields(list.toArray(new String[0]));
			
	    Map<String, String> jsonSchemes = (Map<String, String>) jsonData.get("schemes");
			
			for(Map.Entry<String, String> jsonScheme: jsonSchemes.entrySet()) {
				addSchema(Integer.parseInt(jsonScheme.getKey()), Schema.parse(jsonScheme.getValue()));
			}
			
			setSorting(Sorting.fromJSON((String) jsonData.get("sorting"), mapper));
		} catch(Exception e){
			throw new CoGrouperException(e);
		}
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
}

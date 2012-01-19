package com.datasalt.pangool;

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

import com.datasalt.pangool.io.tuple.ITuple.InvalidFieldException;

/**
 * 
 * @author pere
 *
 */
public class PangoolConfig {

	final static String CONF_PANGOOL_CONF = PangoolConfig.class.getName() + ".pangool.conf";

	private Sorting sorting;
	private Map<Integer, Schema> schemes; // key is schema Id
	private List<String> groupByFields;
	private String rollupFrom;
	
	private Schema commonOrderedSchema;
	private Map<Integer, Schema> specificOrderedSchemas;

	PangoolConfig() {
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
	
	public static void setPangoolConfig(PangoolConfig config, Configuration conf) throws JsonGenerationException, JsonMappingException, IOException {
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
	
	public String toString() {
		StringBuilder b = new StringBuilder();
		b.append("sorting: ").append(sorting.toString()).append(" ");
		b.append("schemes: ").append(schemes.toString()).append(" ");
		b.append("groupByFields: ").append(groupByFields).append(" ");
		b.append("rollupFrom: ").append(rollupFrom);
		return b.toString();
	}

	@SuppressWarnings("unchecked")
	void fromJSON(String json, ObjectMapper mapper) throws JsonParseException, JsonMappingException, IOException, NumberFormatException, CoGrouperException, InvalidFieldException {
		HashMap<String, Object> jsonData = mapper.readValue(json, HashMap.class);
		
		setRollupFrom((String) jsonData.get("rollupFrom"));
		
		ArrayList<String> list = (ArrayList<String>) jsonData.get("groupByFields");
		setGroupByFields(list.toArray(new String[0]));
		
    Map<String, String> jsonSchemes = (Map<String, String>) jsonData.get("schemes");
		
		for(Map.Entry<String, String> jsonScheme: jsonSchemes.entrySet()) {
			addSchema(Integer.parseInt(jsonScheme.getKey()), Schema.parse(jsonScheme.getValue()));
		}
		
		setSorting(Sorting.fromJSON((String) jsonData.get("sorting"), mapper));
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

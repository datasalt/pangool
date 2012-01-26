package com.datasalt.avrool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * 
 * @author pere
 *
 */
public class CoGrouperConfig {

	final static String CONF_PANGOOL_CONF = CoGrouperConfig.class.getName() + ".pangool.conf";

	Ordering commonOrdering;
	Schema.Field.Order interSourcesOrdering;
	Map<String,Ordering> particularOrderings = new HashMap<String,Ordering>();
	
	Map<String, Schema> schemasBySource = new HashMap<String,Schema>();
	private List<String> groupByFields;
	private String rollupFrom;
	
	CoGrouperConfig() {
	}

	void setCommonOrdering(Ordering ordering) {
		this.commonOrdering = ordering;
	}

	public Ordering getCommonSorting() {
  	return commonOrdering;
  }

	public List<String> getGroupByFields() {
  	return groupByFields;
  }

	public String getRollupFrom() {
  	return rollupFrom;
  }

	void addSource(String sourceName, Schema schema) {
		schemasBySource.put(sourceName, schema);
	}

	void setGroupByFields(String... groupByFields) {
		this.groupByFields = Collections.unmodifiableList(Arrays.asList(groupByFields));
	}

	void setRollupFrom(String rollupFrom) {
		this.rollupFrom = rollupFrom;
	}

	public Schema getSchemaBySource(String source){
		return schemasBySource.get(source);
	}
	
	public static void toConfig(CoGrouperConfig config, Configuration conf) throws JsonGenerationException, JsonMappingException, IOException {
		ObjectMapper jsonSerDe = new ObjectMapper();
		conf.set(CONF_PANGOOL_CONF, config.toStringAsJSON(jsonSerDe));
	}

	
	
	public String toString() {
		StringBuilder b = new StringBuilder();
		b.append("schemasBySource: ").append(schemasBySource.toString()).append("\n");
		b.append("groupByFields: ").append(groupByFields).append("\n");
		b.append("rollupFrom: ").append(rollupFrom);
		b.append("commonOrdering: ").append(commonOrdering.toString()).append("\n");
		b.append("interSourcesOrdering: ").append(interSourcesOrdering.toString()).append("\n");
		b.append("individualSourcesOrdering: ").append(particularOrderings.toString()).append("\n");
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
			
	    Map<String, String> jsonSchemes = (Map<String, String>) jsonData.get("schemasBySource");
			
			for(Map.Entry<String, String> jsonScheme: jsonSchemes.entrySet()) {
				addSource(jsonScheme.getKey(), Schema.parse(jsonScheme.getValue()));
			}
			
			setCommonOrdering(Ordering.parse((String) jsonData.get("commonOrdering")));
		} catch(Exception e){
			throw new CoGrouperException(e);
		}
	}
	
	public String toStringAsJSON(ObjectMapper mapper) throws JsonGenerationException, JsonMappingException, IOException {
		Map<String, Object> jsonableData = new HashMap<String, Object>();
		jsonableData.put("commonOrdering", commonOrdering.toString());
		Map<String, String> jsonableSchemes = new HashMap<String, String>();
		
		for(Map.Entry<String, Schema> schemaEntry : schemasBySource.entrySet()) {
			jsonableSchemes.put(schemaEntry.getKey(), schemaEntry.getValue().toString());
		}
		
		jsonableData.put("schemasBySource", jsonableSchemes);
		jsonableData.put("groupByFields", groupByFields);
		jsonableData.put("rollupFrom", rollupFrom);
		return mapper.writeValueAsString(jsonableData);
	}	
}

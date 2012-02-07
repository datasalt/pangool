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
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;


public class CoGrouperConfig {

	final static String CONF_PANGOOL_CONF = CoGrouperConfig.class.getName() + ".pangool.conf";

	private Sorting sorting;
	private Map<Integer, Schema> schemas; // key is schema Id
	private List<String> groupByFields;
	private String rollupFrom;
	private SerializationInfo serInfo;
	private String sourceField;
	
	CoGrouperConfig() {
		schemas = new HashMap<Integer, Schema>();
	}

	void setSorting(Sorting sorting) {
		this.sorting = sorting;
	}
	
	void setSourceField(String field){
		this.sourceField = field;
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

	void addSchema(int schemaId, Schema schema) {
		schemas.put(schemaId, schema);
	}

	void setGroupByFields(String... groupByFields) {
		this.groupByFields = Collections.unmodifiableList(Arrays.asList(groupByFields));
	}

	void setRollupFrom(String rollupFrom) {
		this.rollupFrom = rollupFrom;
	}

	public Map<Integer, Schema> getSchemas() {
		return schemas;
	}
	
	public static void set(CoGrouperConfig config, Configuration conf) throws JsonGenerationException, JsonMappingException, IOException {
		ObjectMapper jsonSerDe = new ObjectMapper();
		conf.set(CONF_PANGOOL_CONF, config.toStringAsJSON(jsonSerDe));
	}
	
	/**
	 * Get the schema of this source.
	 * 
	 * @param tuple
	 */
	public Schema getSchema(int sourceId) {
		return getSchemas().get(sourceId);
	}

	public String toString() {
		StringBuilder b = new StringBuilder();
		b.append("sorting: ").append(sorting.toString()).append(" ");
		b.append("schemas: ").append(schemas.toString()).append(" ");
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
			
	    Map<String, String> jsonSchemes = (Map<String, String>) jsonData.get("schemas");
			
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
		
		for(Map.Entry<Integer, Schema> schemaEntry : schemas.entrySet()) {
			jsonableSchemes.put(schemaEntry.getKey() + "", schemaEntry.getValue().toString());
		}
		
		jsonableData.put("schemas", jsonableSchemes);
		jsonableData.put("groupByFields", groupByFields);
		jsonableData.put("rollupFrom", rollupFrom);
		return mapper.writeValueAsString(jsonableData);
	}	
	
	public SerializationInfo getSerializationInfo(){
		if (serInfo == null){
			try{
				serInfo = new SerializationInfo(this);
			} catch(Exception e){
				throw new RuntimeException(e);
			}
		}
		return serInfo;
	}
	
	public String getSourceField(){
		return sourceField;
	}
	
	
}

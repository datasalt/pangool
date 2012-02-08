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

	private Ordering commonSorting;
	private Map<String, Ordering> secondarySortings; 
	private Map<String, Schema> schemas; // key is schema Id
	private List<String> groupByFields;
	private String rollupFrom;
	private SerializationInfo serInfo;
	private String sourceField;
	
	CoGrouperConfig() {
		schemas = new HashMap<String, Schema>();
	}

	void setCommonOrder(Ordering sorting) {
		this.commonSorting = sorting;
	}
	
	void addSecondaryOrder(String sourceName,Ordering sorting){
		this.secondarySortings.put(sourceName, sorting);
	}
	
	public List<String> getGroupByFields() {
  	return groupByFields;
  }

	public String getRollupFrom() {
  	return rollupFrom;
  }

	void addSource(Schema schema) throws CoGrouperException {
		validateSchema(schema);
		schemas.put(schema.getName(),schema);
	}
	
	private void validateSchema( Schema schema) throws CoGrouperException {
		if(schema == null) {
			throw new CoGrouperException("Schema must not be null");
		} else if(schema.getName() == null){
			throw new CoGrouperException("Schema name must be set");
		}
		
		if(getSources().containsKey(schema.getName())) {
			throw new CoGrouperException("Schema already present: " + schema.getName());
		}

		
	}

	void setGroupByFields(String... groupByFields) {
		this.groupByFields = Collections.unmodifiableList(Arrays.asList(groupByFields));
	}

	void setRollupFrom(String rollupFrom) {
		this.rollupFrom = rollupFrom;
	}

	public Map<String, Schema> getSources() {
		return schemas;
	}
	
	public static void set(CoGrouperConfig config, Configuration conf) throws JsonGenerationException, JsonMappingException, IOException {
		ObjectMapper jsonSerDe = new ObjectMapper();
		conf.set(CONF_PANGOOL_CONF, config.toStringAsJSON(jsonSerDe));
	}
	
	public Schema getSource(String sourceId) {
		return getSources().get(sourceId);
	}

	public String toString() {
		StringBuilder b = new StringBuilder();
		
		b.append("schemas: ").append(schemas.toString()).append("\n");
		b.append("groupByFields: ").append(groupByFields).append("\n");
		b.append("commonSort: ").append(commonSorting.toString()).append("\n");
		b.append("secondarySort: ").append(secondarySortings.toString()).append("\n");
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
				addSource( Schema.parse(jsonScheme.getValue()));
			}
			
			setSorting(Sorting.fromJSON((String) jsonData.get("sorting"), mapper));
		} catch(Exception e){
			throw new CoGrouperException(e);
		}
	}
	
	public String toStringAsJSON(ObjectMapper mapper) throws JsonGenerationException, JsonMappingException, IOException {
		Map<String, Object> jsonableData = new HashMap<String, Object>();
		jsonableData.put("commonSorting", commonSorting.toStringAsJSON(mapper));
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

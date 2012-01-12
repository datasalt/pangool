package com.datasalt.pangolin.pangool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.datasalt.pangolin.grouper.GrouperException;

public class PangoolConfig {

	private Sorting sorting;
	private Map<String, Schema> schemes; // key is schema Id
	private String[] groupByFields;
	private String rollupFrom;

	PangoolConfig() {
		schemes = new HashMap<String, Schema>();
	}

	void setSorting(Sorting sorting) {
		this.sorting = sorting;
	}

	Sorting getSorting() {
  	return sorting;
  }

	String[] getGroupByFields() {
  	return groupByFields;
  }

	String getRollupFrom() {
  	return rollupFrom;
  }

	void addSchema(String schemaId, Schema schema) {
		schemes.put(schemaId, schema);
	}

	void setGroupByFields(String[] groupByFields) {
		this.groupByFields = groupByFields;
	}

	void setRollupFrom(String rollupFrom) {
		this.rollupFrom = rollupFrom;
	}

	Map<String, Schema> getSchemes() {
		return schemes;
	}

	public String toString() {
		StringBuilder b = new StringBuilder();
		b.append("sorting: ").append(sorting.toString()).append("\n");
		b.append("schemes: ").append(schemes.toString()).append("\n");
		b.append("groupByFields: ").append(Arrays.toString(groupByFields)).append("\n");
		b.append("rollupFrom: ").append(rollupFrom);
		return b.toString();
	}

	public String toStringAsJSON(ObjectMapper mapper) throws JsonGenerationException, JsonMappingException, IOException {
		Map<String, Object> jsonableData = new HashMap<String, Object>();
		jsonableData.put("sorting", sorting.toStringAsJSON(mapper));
		Map<String, String> jsonableSchemes = new HashMap<String, String>();
		for(Map.Entry<String, Schema> schemaEntry : schemes.entrySet()) {
			jsonableSchemes.put(schemaEntry.getKey(), schemaEntry.getValue().toString());
		}
		jsonableData.put("schemes", jsonableSchemes);
		jsonableData.put("groupByFields", groupByFields);
		jsonableData.put("rollupFrom", rollupFrom);
		return mapper.writeValueAsString(jsonableData);
	}
	
	@SuppressWarnings("unchecked")
  static PangoolConfig fromJSON(String json, ObjectMapper mapper) throws JsonParseException, JsonMappingException, IOException, GrouperException {
		PangoolConfig config = new PangoolConfig();
		HashMap<String, Object> jsonData = mapper.readValue(json, HashMap.class);
		config.setRollupFrom((String) jsonData.get("rollupFrom"));
		ArrayList<String> list = (ArrayList<String>) jsonData.get("groupByFields");
		config.setGroupByFields(list.toArray(new String[0]));
		config.setSorting(Sorting.fromJSON((String) jsonData.get("sorting"), mapper));
		Map<String, String> jsonSchemes = (Map<String, String>) jsonData.get("schemes");
		for(Map.Entry<String, String> jsonScheme: jsonSchemes.entrySet()) {
			config.addSchema(jsonScheme.getKey(), Schema.parse(jsonScheme.getValue()));
		}
		return config;
	}
}

package com.datasalt.pangolin.pangool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class PangoolConfig {

	private Sorting sorting;
	private Map<Integer, Schema> schemes; // key is schema Id
	private List<String> groupByFields;
	private String rollupFrom;

	PangoolConfig() {
		schemes = new HashMap<Integer, Schema>();
	}

	void setSorting(Sorting sorting) {
		this.sorting = sorting;
	}

	Sorting getSorting() {
  	return sorting;
  }

	List<String> getGroupByFields() {
  	return groupByFields;
  }

	String getRollupFrom() {
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

	Map<Integer, Schema> getSchemes() {
		return schemes;
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
		list = (ArrayList<String>) jsonData.get("customPartitionerFields");
		Map<String, String> jsonSchemes = (Map<String, String>) jsonData.get("schemes");
		for(Map.Entry<String, String> jsonScheme: jsonSchemes.entrySet()) {
			config.addSchema(Integer.parseInt(jsonScheme.getKey()), Schema.parse(jsonScheme.getValue()));
		}
		config.setSorting(Sorting.fromJSON((String) jsonData.get("sorting"), mapper));
		return config;
	}
}

package com.datasalt.pangolin.pangool;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

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
}

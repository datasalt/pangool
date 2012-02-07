package com.datasalt.pangool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Encapsulates the sorting configuration
 * 
 */
public class Sorting {

	private SortCriteria commonSortCriteria;
	private Map<Integer, SortCriteria> specificSortCriterias; // key is source Id

	Sorting(SortCriteria sortCriteria, Map<Integer, SortCriteria> specificSortCriterias) {
		this.commonSortCriteria = sortCriteria;
		this.specificSortCriterias = specificSortCriterias;
	}

	public SortCriteria getCommonSortCriteria() {
		return commonSortCriteria;
	}

	public Map<Integer, SortCriteria> getSpecificSortCriterias() {
		return specificSortCriterias;
	}

	private Map<String, Object> getJsonableData() {
		Map<String, Object> jsonableData = new HashMap<String, Object>();
		Map<String, String> jsonableSpecificSortCriterias = new HashMap<String, String>();
		for(Map.Entry<Integer, SortCriteria> sortCriteria : specificSortCriterias.entrySet()) {
			jsonableSpecificSortCriterias.put(sortCriteria.getKey() + "", sortCriteria.getValue().toString());
		}
		jsonableData.put("commonSortCriteria", commonSortCriteria.toString());
		jsonableData.put("specificSortCriterias", jsonableSpecificSortCriterias);
		return jsonableData;
	}

	public String toString() {
		return getJsonableData().toString();
	}

	public String toStringAsJSON(ObjectMapper objectMapper) throws JsonGenerationException, JsonMappingException, IOException {
		return objectMapper.writeValueAsString(getJsonableData());
	}
	
	@SuppressWarnings("unchecked")
  static Sorting fromJSON(String json, ObjectMapper mapper) throws JsonParseException, JsonMappingException, IOException, CoGrouperException {
		Map<String, Object> jsonData = mapper.readValue(json, HashMap.class);
		SortCriteria sortCriteria = SortCriteria.parse((String) jsonData.get("commonSortCriteria"));
		
		
		
		Map<String, String> jsonSpecificSortCriterias = (Map<String, String>) jsonData.get("specificSortCriterias");
		Map<Integer, SortCriteria> specificSortCriterias = new HashMap<Integer, SortCriteria>();
		for(Map.Entry<String, String> jsonSpecificSortCriteria: jsonSpecificSortCriterias.entrySet()) {
			specificSortCriterias.put(Integer.parseInt(jsonSpecificSortCriteria.getKey()), SortCriteria.parse(jsonSpecificSortCriteria.getValue()));
		}
		Sorting sorting = new Sorting(sortCriteria,  specificSortCriterias);
		return sorting;
	}
	
	public static Sorting parse(String sortingStr) throws CoGrouperException {
		SortCriteria sortCriteria = SortCriteria.parse(sortingStr);
		Map<Integer, SortCriteria> specificSortCriterias = new HashMap<Integer, SortCriteria>();
		Sorting sorting = new Sorting(sortCriteria,  specificSortCriterias);
		return sorting;
	}
}

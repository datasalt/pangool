package com.datasalt.pangolin.pangool;

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
 * @author pere
 * 
 */
public class Sorting {

	private SortCriteria sortCriteria;
	private Map<Integer, SortCriteria> secondarySortCriterias; // key is source Id

	Sorting(SortCriteria sortCriteria, Map<Integer, SortCriteria> secondarySortCriterias) {
		this.sortCriteria = sortCriteria;
		this.secondarySortCriterias = secondarySortCriterias;
	}

	public SortCriteria getSortCriteria() {
		return sortCriteria;
	}

	public Map<Integer, SortCriteria> getSecondarySortCriterias() {
		return secondarySortCriterias;
	}

	public SortCriteria getSecondarySortCriteriaByName(Integer schemaName) {
		return secondarySortCriterias.get(schemaName);
	}
	
	private Map<String, Object> getJsonableData() {
		Map<String, Object> jsonableData = new HashMap<String, Object>();
		Map<String, String> jsonableSecondarySortCriterias = new HashMap<String, String>();
		for(Map.Entry<Integer, SortCriteria> sortCriteria : secondarySortCriterias.entrySet()) {
			jsonableSecondarySortCriterias.put(sortCriteria.getKey() + "", sortCriteria.getValue().toString());
		}
		jsonableData.put("sortCriteria", sortCriteria.toString());
		jsonableData.put("secondarySortCriterias", jsonableSecondarySortCriterias);
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
		SortCriteria sortCriteria = SortCriteria.parse((String) jsonData.get("sortCriteria"));
		
		Map<String, String> jsonSecondarySortCriterias = (Map<String, String>) jsonData.get("secondarySortCriterias");
		Map<Integer, SortCriteria> secondarySortCriterias = new HashMap<Integer, SortCriteria>();
		for(Map.Entry<String, String> jsonSecondarySortCriteria: jsonSecondarySortCriterias.entrySet()) {
			secondarySortCriterias.put(Integer.parseInt(jsonSecondarySortCriteria.getKey()), SortCriteria.parse(jsonSecondarySortCriteria.getValue()));
		}
		Sorting sorting = new Sorting(sortCriteria, secondarySortCriterias);
		return sorting;
	}
	
	public static Sorting parse(String sortingStr) throws CoGrouperException {
		Map<Integer, SortCriteria> secondarySortCriterias = new HashMap<Integer, SortCriteria>();
		SortCriteria sortCriteria = SortCriteria.parse(sortingStr);
		Sorting sorting = new Sorting(sortCriteria, secondarySortCriterias);
		return sorting;
	}
}

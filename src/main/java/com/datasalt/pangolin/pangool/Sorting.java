package com.datasalt.pangolin.pangool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.datasalt.pangolin.grouper.GrouperException;
import com.datasalt.pangolin.pangool.SortCriteria;

/**
 * Encapsulates the sorting configuration
 * 
 * @author pere
 * 
 */
public class Sorting {

	private SortCriteria sortCriteria;
	private Map<String, SortCriteria> secondarySortCriterias; // key is source Id

	Sorting(SortCriteria sortCriteria, Map<String, SortCriteria> secondarySortCriterias) {
		this.sortCriteria = sortCriteria;
		this.secondarySortCriterias = secondarySortCriterias;
	}

	public SortCriteria getSortCriteria() {
		return sortCriteria;
	}

	public Map<String, SortCriteria> getSecondarySortCriterias() {
		return secondarySortCriterias;
	}

	public SortCriteria getSecondarySortCriteriaByName(String schemaName) {
		return secondarySortCriterias.get(schemaName);
	}
	
	private Map<String, Object> getJsonableData() {
		Map<String, Object> jsonableData = new HashMap<String, Object>();
		Map<String, String> jsonableSecondarySortCriterias = new HashMap<String, String>();
		for(Map.Entry<String, SortCriteria> entry : secondarySortCriterias.entrySet()) {
			jsonableSecondarySortCriterias.put(entry.getKey(), entry.getValue().toString());
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
  static Sorting fromJSON(String json, ObjectMapper mapper) throws JsonParseException, JsonMappingException, IOException, GrouperException {
		Map<String, Object> jsonData = mapper.readValue(json, HashMap.class);
		SortCriteria sortCriteria = SortCriteria.parse((String) jsonData.get("sortCriteria"));
		Map<String, String> jsonSecondarySortCriterias = (Map<String, String>) jsonData.get("secondarySortCriterias");
		Map<String, SortCriteria> secondarySortCriterias = new HashMap<String, SortCriteria>();
		for(Map.Entry<String, String> jsonSecondarySortCriteria: jsonSecondarySortCriterias.entrySet()) {
			secondarySortCriterias.put(jsonSecondarySortCriteria.getKey(), SortCriteria.parse(jsonSecondarySortCriteria.getValue()));
		}
		Sorting sorting = new Sorting(sortCriteria, secondarySortCriterias);
		return sorting;
	}
}

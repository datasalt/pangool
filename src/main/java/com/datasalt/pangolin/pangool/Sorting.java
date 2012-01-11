package com.datasalt.pangolin.pangool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

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
		jsonableData.put("secondarySortCriterias", jsonableSecondarySortCriterias.toString());
		return jsonableData;
	}

	public String toString() {
		return getJsonableData().toString();
	}

	public String toStringAsJSON(ObjectMapper objectMapper) throws JsonGenerationException, JsonMappingException, IOException {
		return objectMapper.writeValueAsString(getJsonableData());
	}
}

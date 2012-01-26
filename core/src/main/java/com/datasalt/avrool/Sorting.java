package com.datasalt.avrool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
//
///**
// * Encapsulates the sorting configuration
// * 
// * 
// * 
// */
//public class Sorting {
//
//	private Schema commonCriteria;
//	//private boolean sourceIdFieldContained;
//	private Map<Integer, SortCriteria> specificSortCriterias; // key is source Id
//
//	Sorting(SortCriteria sortCriteria, boolean sourceIdFieldContained, Map<Integer, SortCriteria> specificSortCriterias) {
//		this.commonCriteria = sortCriteria;
//		this.sourceIdFieldContained = sourceIdFieldContained;
//		this.specificSortCriterias = specificSortCriterias;
//	}
//
//	public boolean isSourceIdFieldContained() {
//  	return sourceIdFieldContained;
//  }
//
//	public SortCriteria getSortCriteria() {
//		return commonCriteria;
//	}
//
//	public Map<Integer, SortCriteria> getSpecificSortCriterias() {
//		return specificSortCriterias;
//	}
//
//	public SortCriteria getSpecificCriteriaByName(Integer schemaName) {
//		return specificSortCriterias.get(schemaName);
//	}
//	
//	private Map<String, Object> getJsonableData() {
//		Map<String, Object> jsonableData = new HashMap<String, Object>();
//		Map<String, String> jsonableSpecificSortCriterias = new HashMap<String, String>();
//		for(Map.Entry<Integer, SortCriteria> sortCriteria : specificSortCriterias.entrySet()) {
//			jsonableSpecificSortCriterias.put(sortCriteria.getKey() + "", sortCriteria.getValue().toString());
//		}
//		jsonableData.put("commonCriteria", commonCriteria.toString());
//		jsonableData.put("sourceIdFieldContained", sourceIdFieldContained);
//		jsonableData.put("specificSortCriterias", jsonableSpecificSortCriterias);
//		return jsonableData;
//	}
//
//	public String toString() {
//		return getJsonableData().toString();
//	}
//
//	public String toStringAsJSON(ObjectMapper objectMapper) throws JsonGenerationException, JsonMappingException, IOException {
//		return objectMapper.writeValueAsString(getJsonableData());
//	}
//	
//	@SuppressWarnings("unchecked")
//  static Sorting fromJSON(String json, ObjectMapper mapper) throws JsonParseException, JsonMappingException, IOException, CoGrouperException {
//		Map<String, Object> jsonData = mapper.readValue(json, HashMap.class);
//		SortCriteria sortCriteria = SortCriteria.parse((String) jsonData.get("commonCriteria"));
//		
//		boolean sourceIdFieldContained = (Boolean) jsonData.get("sourceIdFieldContained");
//		
//		Map<String, String> jsonSpecificSortCriterias = (Map<String, String>) jsonData.get("specificSortCriterias");
//		Map<Integer, SortCriteria> specificSortCriterias = new HashMap<Integer, SortCriteria>();
//		for(Map.Entry<String, String> jsonSpecificSortCriteria: jsonSpecificSortCriterias.entrySet()) {
//			specificSortCriterias.put(Integer.parseInt(jsonSpecificSortCriteria.getKey()), SortCriteria.parse(jsonSpecificSortCriteria.getValue()));
//		}
//		Sorting sorting = new Sorting(sortCriteria, sourceIdFieldContained, specificSortCriterias);
//		return sorting;
//	}
//	
//	public static Sorting parse(String sortingStr) throws CoGrouperException {
//		SortCriteria sortCriteria = SortCriteria.parse(sortingStr);
//		Map<Integer, SortCriteria> specificSortCriterias = new HashMap<Integer, SortCriteria>();
//		Sorting sorting = new Sorting(sortCriteria, sortCriteria.hasSourceIdField(), specificSortCriterias);
//		return sorting;
//	}
//}

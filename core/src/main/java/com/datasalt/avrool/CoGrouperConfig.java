package com.datasalt.avrool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field.Order;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.datasalt.avrool.Ordering.SortElement;

public class CoGrouperConfig {

	public final static String CONF_PANGOOL_CONF = CoGrouperConfig.class.getName() + ".pangool.conf";

	Ordering commonOrdering;
	Schema.Field.Order interSourcesOrdering = Order.IGNORE;
	Map<String,Ordering> particularOrderings = new LinkedHashMap<String,Ordering>();
	
	Map<String, Schema> schemasBySource = new LinkedHashMap<String,Schema>();
	private List<String> groupByFields;
	private String rollupFrom;
	
	CoGrouperConfig() {
	}

	void setCommonOrdering(Ordering ordering) {
		this.commonOrdering = ordering;
	}

	public Ordering getCommonSorting() {
  	return commonOrdering;
  }

	public List<String> getGroupByFields() {
  	return groupByFields;
  }
	
	public List<String> getRollupBaseFields(){
		if (rollupFrom == null){
			return getGroupByFields();
		}
		
		List<String> result = new ArrayList<String>();
		for (SortElement element : commonOrdering.getElements()){
			result.add(element.getName());
			if (element.getName().equals(rollupFrom)){
				break;
			}
		}
		return result;
	}

	public String getRollupFrom() {
  	return rollupFrom;
  }

	void addSource(Schema schema) {
		schemasBySource.put(schema.getFullName(), schema);
	}

	void setGroupByFields(String... groupByFields) {
		this.groupByFields = Collections.unmodifiableList(Arrays.asList(groupByFields));
	}

	void setRollupFrom(String rollupFrom) {
		this.rollupFrom = rollupFrom;
	}

	public Schema getSchemaBySource(String source){
		return schemasBySource.get(source);
	}
	
	public Map<String,Schema> getSchemasBySource(){
		return schemasBySource;
	}
	
	public static void toConfig(CoGrouperConfig config, Configuration conf) throws JsonGenerationException, JsonMappingException, IOException {
		ObjectMapper jsonSerDe = new ObjectMapper();
		conf.set(CONF_PANGOOL_CONF, config.toJSON(jsonSerDe));
	}

	
	
	public String toString() {
		StringBuilder b = new StringBuilder();
		b.append("schemasBySource: ").append(schemasBySource.toString()).append("\n");
		b.append("groupByFields: ").append(groupByFields).append("\n");
		b.append("rollupFrom: ").append(rollupFrom).append("\n");
		b.append("commonOrdering: ").append(commonOrdering.toString()).append("\n");
		b.append("interSourcesOrdering: ").append(interSourcesOrdering.toString()).append("\n");
		b.append("particularSourcesOrdering: ").append(particularOrderings.toString()).append("\n");
		return b.toString();
	}

	@SuppressWarnings("unchecked")
	public static CoGrouperConfig parseJSON(String json, ObjectMapper mapper) throws CoGrouperException {
		
		if (json == null){
			throw new CoGrouperException("Non existing pangool config set in Configuration");
		}
		
		CoGrouperConfig result = new CoGrouperConfig();
		
		try{
			HashMap<String, Object> jsonData = mapper.readValue(json, HashMap.class);
			result.setRollupFrom((String) jsonData.get("rollupFrom"));
			ArrayList<String> list = (ArrayList<String>) jsonData.get("groupByFields");
			result.setGroupByFields(list.toArray(new String[0]));
	    Map<String, String> jsonSources = (Map<String, String>) jsonData.get("schemasBySource");
	    result.interSourcesOrdering = Schema.Field.Order.valueOf((String) jsonData.get("interSourcesOrdering"));
	    
			for(Map.Entry<String, String> jsonSchema: jsonSources.entrySet()) {
				result.addSource(Schema.parse(jsonSchema.getValue()));
			}
			List<Map> listOrderings = (List<Map>)jsonData.get("commonOrdering");
			
			
			
			result.setCommonOrdering(new Ordering(mapsToSortElements(listOrderings)));
			Map<String, List<SortElement>> jsonParticularOrderings = (Map<String, List<SortElement>>) jsonData.get("particularOrderings");
			
			for(Map.Entry<String, List<SortElement>> entry: jsonParticularOrderings.entrySet()) {
				result.particularOrderings.put(entry.getKey(), new Ordering(mapsToSortElements((List)entry.getValue())));
			}
			return result;
		} catch(Exception e){
			throw new CoGrouperException(e);
		}
	}
	
	private static List<SortElement> mapsToSortElements(List<Map> maps){
		List<SortElement> result = new ArrayList<SortElement>();
		for (Map map : maps){
			SortElement element = new SortElement((String)map.get("name"),Order.valueOf((String)map.get("order")));
			result.add(element);
		}
	
		return result;
	}
	
	
	public static CoGrouperConfig get(Configuration conf) throws CoGrouperException {
		ObjectMapper jsonSerDe = new ObjectMapper();
			String serialized =conf.get(CoGrouperConfig.CONF_PANGOOL_CONF);
	    return (serialized == null || serialized.isEmpty()) ? null : CoGrouperConfig.parseJSON(serialized, jsonSerDe);
	}
	
	
	
	public String toJSON(ObjectMapper mapper) throws JsonGenerationException, JsonMappingException, IOException {
		Map<String, Object> jsonableData = new HashMap<String, Object>();
		
		Map<String, String> jsonableSources = new HashMap<String, String>();
		
		for(Map.Entry<String, Schema> schemaEntry : schemasBySource.entrySet()) {
			jsonableSources.put(schemaEntry.getKey(), schemaEntry.getValue().toString());
		}
		
		jsonableData.put("schemasBySource", jsonableSources);
		jsonableData.put("commonOrdering", commonOrdering.getElements());
		jsonableData.put("interSourcesOrdering", interSourcesOrdering);
		
		Map<String, List> jsonableParticularOrderings = new HashMap<String, List>();
		
		for(Map.Entry<String, Ordering> entry : particularOrderings.entrySet()) {
			jsonableParticularOrderings.put(entry.getKey(), entry.getValue().getElements());
		}
		
		jsonableData.put("particularOrderings", jsonableParticularOrderings);
		jsonableData.put("groupByFields", groupByFields);
		jsonableData.put("rollupFrom", rollupFrom);
		return mapper.writeValueAsString(jsonableData);
	}	
	
	
}

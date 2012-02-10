package com.datasalt.pangool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.map.ObjectMapper;

import com.datasalt.pangool.SortBy.Order;
import com.datasalt.pangool.SortBy.SortElement;


public class CoGrouperConfig {

	private final static String CONF_PANGOOL_CONF = CoGrouperConfig.class.getName() + ".pangool.conf";

	private SortBy commonOrdering;
	private Map<String,SortBy> particularOrderings = new LinkedHashMap<String,SortBy>();
	
	private LinkedHashMap<String, Schema> sourceSchemas = new LinkedHashMap<String,Schema>();
	private List<String> groupByFields;
	private String rollupFrom;
	
	private SerializationInfo serInfo;
	
	public void setParticularOrdering(String sourceName,SortBy ordering){
		particularOrderings.put(sourceName,ordering);
	}
	
	public Map<String,SortBy> getSecondarySortBys(){
		return particularOrderings;
	}
	
	
	CoGrouperConfig() {
	}
	
	public SerializationInfo getSerializationInfo(){
		if (serInfo == null){
			try{
				this.serInfo = new SerializationInfo(this);
			} catch(Exception e){
				throw new RuntimeException(e);
			}
		}
		return this.serInfo;
	}

	public int getNumSources(){
		return sourceSchemas.size();
	}

	void setCommonSortBy(SortBy ordering) {
		this.commonOrdering = ordering;
	}

	public SortBy getCommonSortBy() {
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
		sourceSchemas.put(schema.getName(), schema);
	}

	void setGroupByFields(String... groupByFields) {
		this.groupByFields = Collections.unmodifiableList(Arrays.asList(groupByFields));
	}

	void setRollupFrom(String rollupFrom) {
		this.rollupFrom = rollupFrom;
	}

	public Schema getSourceSchema(String source){
		return sourceSchemas.get(source);
	}
	
	public Map<String,Schema> getSourceSchemas(){
		return sourceSchemas;
	}
	
	public String toString() {
		StringBuilder b = new StringBuilder();
		b.append("sourceSchemas: ").append(sourceSchemas.toString()).append("\n");
		b.append("groupByFields: ").append(groupByFields).append("\n");
		b.append("rollupFrom: ").append(rollupFrom).append("\n");
		b.append("commonOrdering: ").append(commonOrdering.toString()).append("\n");
		b.append("particularSourcesOrdering: ").append(particularOrderings.toString()).append("\n");
		return b.toString();
	}

	@SuppressWarnings("unchecked")
	public static CoGrouperConfig parseJSON(String json, ObjectMapper mapper) throws CoGrouperException {
		
		if (json == null){
			throw new CoGrouperException("Non existing pangool grouperConf set in Configuration");
		}
		
		CoGrouperConfig result = new CoGrouperConfig();
		
		try{
			HashMap<String, Object> jsonData = mapper.readValue(json, HashMap.class);
			result.setRollupFrom((String) jsonData.get("rollupFrom"));
			ArrayList<String> list = (ArrayList<String>) jsonData.get("groupByFields");
			result.setGroupByFields(list.toArray(new String[list.size()]));
	    LinkedHashMap<String, String> jsonSources = (LinkedHashMap<String, String>) jsonData.get("sourceSchemas");

	    
			for(Map.Entry<String, String> jsonSchema: jsonSources.entrySet()) {
				result.addSource(Schema.parse(jsonSchema.getValue()));
			}
			List<Map> listOrderings = (List<Map>)jsonData.get("commonOrdering");
			
			result.setCommonSortBy(new SortBy(mapsToSortElements(listOrderings)));
			Map<String, List<SortElement>> jsonParticularOrderings = (Map<String, List<SortElement>>) jsonData.get("particularOrderings");
			
			for(Map.Entry<String, List<SortElement>> entry: jsonParticularOrderings.entrySet()) {
				result.particularOrderings.put(entry.getKey(), new SortBy(mapsToSortElements((List)entry.getValue())));
			}
			return result;
		} catch(Exception e){
			throw new CoGrouperException(e);
		}
	}
	
	private static List<SortElement> mapsToSortElements(List<Map> maps){
		List<SortElement> result = new ArrayList<SortElement>();
		for (Map map : maps){
			//TODO
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
	
	public static void set(CoGrouperConfig grouperConfig,Configuration conf) throws CoGrouperException{
		conf.set(CONF_PANGOOL_CONF, grouperConfig.toJSON());
	}
	
	
	public String toJSON() throws CoGrouperException {
		try{
		ObjectMapper mapper = new ObjectMapper();
		Map<String, Object> jsonableData = new HashMap<String, Object>();
		
		LinkedHashMap<String, String> jsonableSources = new LinkedHashMap<String, String>();
		
		for(Map.Entry<String, Schema> schemaEntry : sourceSchemas.entrySet()) {
			jsonableSources.put(schemaEntry.getKey(), schemaEntry.getValue().toString());
		}
		
		jsonableData.put("sourceSchemas", jsonableSources);
		jsonableData.put("commonOrdering", commonOrdering.getElements());
		//jsonableData.put("interSourcesOrdering", interSourcesOrdering);
		
		Map<String, List> jsonableParticularOrderings = new HashMap<String, List>();
		
		for(Map.Entry<String, SortBy> entry : particularOrderings.entrySet()) {
			jsonableParticularOrderings.put(entry.getKey(), entry.getValue().getElements());
		}
		
		jsonableData.put("particularOrderings", jsonableParticularOrderings);
		jsonableData.put("groupByFields", groupByFields);
		jsonableData.put("rollupFrom", rollupFrom);
		return mapper.writeValueAsString(jsonableData);
		} catch(Exception e){
			throw new CoGrouperException(e);
		}
	}	
	
	
}

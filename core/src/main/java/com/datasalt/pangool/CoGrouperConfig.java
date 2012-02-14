package com.datasalt.pangool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import com.datasalt.pangool.Criteria.Order;
import com.datasalt.pangool.Criteria.SortElement;


public class CoGrouperConfig {

	private final static String CONF_PANGOOL_CONF = CoGrouperConfig.class.getName() + ".pangool.conf";

	static final JsonFactory FACTORY = new JsonFactory();
  static final ObjectMapper MAPPER = new ObjectMapper(FACTORY);

  //private static final int NO_HASHCODE = Integer.MIN_VALUE;

  static {
    FACTORY.enable(JsonParser.Feature.ALLOW_COMMENTS);
    FACTORY.setCodec(MAPPER);
  }
	
	private Criteria commonSortBy;
	
	private List<String> sourceNames=new ArrayList<String>();
	private Map<String,Integer> sourceNameToId = new HashMap<String,Integer>();
	
	private List<Criteria> secondarySortBys=new ArrayList<Criteria>();
	
	private List<Schema> sourceSchemas = new ArrayList<Schema>();
	private List<String> groupByFields;
	private String rollupFrom;
	
	private SerializationInfo serInfo;
	
	void setSecondarySortBy(String sourceName,Criteria criteria){
		if (this.secondarySortBys.isEmpty()){
			for (int i = 0; i < getNumSources() ; i++){
				this.secondarySortBys.add(null);
			}
		}
		
		Integer pos = getSourceIdByName(sourceName);
		if (pos == null){
			throw new IllegalArgumentException("Not known source with name '" + sourceName + "'");
		}
		secondarySortBys.set(pos, criteria);
	}
	
	public List<Criteria> getSecondarySortBys(){
		return secondarySortBys;
	}
	
//	if(grouperConf.getRollupFrom() != null) {
//
//		// Check that rollupFrom is contained in groupBy
//
//		if(!grouperConf.getGroupByFields().contains(grouperConf.getRollupFrom())) {
//			throw new CoGrouperException("Rollup from [" + grouperConf.getRollupFrom() + "] not contained in group by fields "
//			    + grouperConf.getGroupByFields());
//		}
//
//		// Check that we are using the appropriate Handler
//
//		if(!(grouperHandler instanceof GroupHandlerWithRollup)) {
//			throw new CoGrouperException("Can't use " + grouperHandler + " with rollup. Please use "
//			    + GroupHandlerWithRollup.class + " instead.");
//		}
//	}
//	
	
	
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

	void setCommonSortBy(Criteria ordering) {
		this.commonSortBy = ordering;
	}

	public Criteria getCommonSortBy() {
  	return commonSortBy;
  }

	public List<String> getGroupByFields() {
  	return groupByFields;
  }
	
	public List<String> getRollupBaseFields(){
		if (rollupFrom == null){
			return getGroupByFields();
		}
		
		List<String> result = new ArrayList<String>();
		for (SortElement element : commonSortBy.getElements()){
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
		if (sourceNames.contains(schema.getName())){
			throw new IllegalArgumentException("There's a schema with that name '" + schema.getName() + "'");
		}
		sourceNameToId.put(schema.getName(),sourceNames.size());
		sourceNames.add(schema.getName());
		sourceSchemas.add(schema);
	}

	void setGroupByFields(String... groupByFields) {
		this.groupByFields = Collections.unmodifiableList(Arrays.asList(groupByFields));
	}

	void setRollupFrom(String rollupFrom) {
		this.rollupFrom = rollupFrom;
	}

	public Schema getSourceSchema(String source){
		Integer id = getSourceIdByName(source);
		return (id == null) ? null : sourceSchemas.get(id);
	}
	
	public Schema getSourceSchema(int sourceId){
		return sourceSchemas.get(sourceId);
	}
	
	public int getSourceIdByName(String name){
		return sourceNameToId.get(name);
	}
	
	
	public List<Schema> getSourceSchemas(){
		return sourceSchemas;
	}
	
	public String toString() {
		StringBuilder b = new StringBuilder();
		b.append("sourceSchemas: ").append(sourceSchemas.toString()).append("\n");
		b.append("groupByFields: ").append(groupByFields).append("\n");
		b.append("rollupFrom: ").append(rollupFrom).append("\n");
		b.append("commonSortBy: ").append(commonSortBy.toString()).append("\n");
		b.append("secondarySortBys: ").append(secondarySortBys.toString()).append("\n");
		return b.toString();
	}

	@SuppressWarnings("unchecked")
	public static CoGrouperConfig parseJSON(String json) throws CoGrouperException {
		ObjectMapper mapper = new ObjectMapper();
		if (json == null){
			throw new CoGrouperException("Non existing pangool grouperConf set in Configuration");
		}
		
		CoGrouperConfig result = new CoGrouperConfig();
		
		try{
			HashMap<String, Object> jsonData = mapper.readValue(json, HashMap.class);
			result.setRollupFrom((String) jsonData.get("rollupFrom"));
			List<String> list = (List<String>) jsonData.get("groupByFields");
			result.setGroupByFields(list.toArray(new String[list.size()]));
	    List<String> jsonSources = (List<String>) jsonData.get("sourceSchemas");
	    
			for(String jsonSchema: jsonSources) {
				result.addSource(Schema.parse(jsonSchema));
			}
			List<Map> listOrderings = (List<Map>)jsonData.get("commonSortBy");
			
			result.setCommonSortBy(new Criteria(mapsToSortElements(listOrderings)));
			List<List> jsonParticularOrderings = (List<List>) jsonData.get("secondarySortBys");
			
			for(List entry: jsonParticularOrderings) {
				result.secondarySortBys.add((entry == null) ? null : new Criteria(mapsToSortElements((List)entry)));
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
			String serialized =conf.get(CoGrouperConfig.CONF_PANGOOL_CONF);
	    return (serialized == null || serialized.isEmpty()) ? null : CoGrouperConfig.parseJSON(serialized);
	}
	
	public static void set(CoGrouperConfig grouperConfig,Configuration conf) throws CoGrouperException{
		conf.set(CONF_PANGOOL_CONF, grouperConfig.toJSON());
	}
	
	
	public String toJSON() throws CoGrouperException {
		try{
		ObjectMapper mapper = new ObjectMapper();
		Map<String, Object> jsonableData = new HashMap<String, Object>();
		
		List<String> jsonableSources = new ArrayList<String>();
		
		for(Schema schema : sourceSchemas) {
			jsonableSources.add(schema.toString());
		}
		
		jsonableData.put("sourceSchemas", jsonableSources);
		jsonableData.put("commonSortBy", commonSortBy.getElements());
		//jsonableData.put("interSourcesOrdering", interSourcesOrdering);
		
		List<List> jsonableParticularOrderings = new ArrayList<List>();
		if (secondarySortBys != null){
			for(Criteria criteria : secondarySortBys) {
				jsonableParticularOrderings.add((criteria == null) ? null : criteria.getElements());
			}
		}
		
		jsonableData.put("secondarySortBys", jsonableParticularOrderings);
		jsonableData.put("groupByFields", groupByFields);
		jsonableData.put("rollupFrom", rollupFrom);
		String result =mapper.writeValueAsString(jsonableData);
		System.out.println("Config serialized : " + result);
		return result;
		} catch(Exception e){
			throw new CoGrouperException(e);
		}
	}	
	
	
}

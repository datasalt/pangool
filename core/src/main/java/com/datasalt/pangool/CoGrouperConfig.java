package com.datasalt.pangool;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import com.datasalt.pangool.Criteria.Order;
import com.datasalt.pangool.Criteria.SortElement;

public class CoGrouperConfig {

	private final static String CONF_PANGOOL_CONF = CoGrouperConfig.class.getName() + ".pangool.conf";

	static final JsonFactory FACTORY = new JsonFactory();
  static final ObjectMapper MAPPER = new ObjectMapper(FACTORY);

  static {
    FACTORY.enable(JsonParser.Feature.ALLOW_COMMENTS);
    FACTORY.setCodec(MAPPER);
  }
	
	
	private List<String> sourceNames=new ArrayList<String>();
	private Map<String,Integer> sourceNameToId = new HashMap<String,Integer>();
	
	private Criteria commonCriteria;
	private List<Criteria> secondaryCriterias=new ArrayList<Criteria>();
	private Order sourcesOrder;
	
	private List<Schema> sourceSchemas = new ArrayList<Schema>();
	private List<String> groupByFields;
	private String rollupFrom;
	
	private SerializationInfo serInfo;
	
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
	
	public Order getSourcesOrder(){
		return sourcesOrder;
	}
	
	public List<Criteria> getSecondarySortBys(){
		return secondaryCriterias;
	}
		
	protected CoGrouperConfig() {
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

	

	public Criteria getCommonCriteria() {
  	return commonCriteria;
  }

	public List<String> getGroupByFields() {
  	return groupByFields;
  }
	
	public List<String> calculateRollupBaseFields(){
		if (rollupFrom == null){
			return getGroupByFields();
		}
		
		List<String> result = new ArrayList<String>();
		for (SortElement element : commonCriteria.getElements()){
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

	void addSource(Schema schema) throws CoGrouperException {
		if (sourceNames.contains(schema.getName())){
			throw new CoGrouperException("There's a schema with that name '" + schema.getName() + "'");
		}
		sourceNameToId.put(schema.getName(),sourceNames.size());
		sourceNames.add(schema.getName());
		sourceSchemas.add(schema);
	}

	
	
	void setGroupByFields(List<String> groupByFields) {
		this.groupByFields = groupByFields;
	}

	void setRollupFrom(String rollupFrom) throws CoGrouperException {
		this.rollupFrom = rollupFrom;
	}
	
	void setCommonCriteria(Criteria ordering) {
		this.commonCriteria = ordering;
	}
	
	void setSourceOrder(Order order){
		this.sourcesOrder = order;
	}
	
	void setSecondarySortBy(String sourceName,Criteria criteria) throws CoGrouperException {
		if (this.secondaryCriterias.isEmpty()){
			for (int i = 0; i < getNumSources() ; i++){
				this.secondaryCriterias.add(null);
			}
		}
		Integer pos = getSourceIdByName(sourceName);
		secondaryCriterias.set(pos, criteria);
	}

	
	
	public static CoGrouperConfig get(Configuration conf) throws CoGrouperException {
			String serialized =conf.get(CoGrouperConfig.CONF_PANGOOL_CONF);
			try{
	    return (serialized == null || serialized.isEmpty()) ? null : CoGrouperConfig.parse(serialized);
			} catch(IOException e){
				throw new CoGrouperException(e);
			}
	}
	
	public static void set(CoGrouperConfig grouperConfig,Configuration conf) throws CoGrouperException{
		conf.set(CONF_PANGOOL_CONF, grouperConfig.toString());
	}
	
	static CoGrouperConfig parse(JsonNode node) throws IOException {
		try{
		CoGrouperConfig result = new CoGrouperConfig();
		Iterator<JsonNode> sources = node.get("sourceSchemas").getElements();
		while (sources.hasNext()){
			JsonNode sourceNode = sources.next();
			result.addSource(Schema.parse(sourceNode));
		}
		
		Iterator<JsonNode> groupFieldsNode = node.get("groupByFields").getElements();
		List<String> groupFields = new ArrayList<String>();
		while (groupFieldsNode.hasNext()){
			groupFields.add(groupFieldsNode.next().getTextValue());
		}
		result.groupByFields = Collections.unmodifiableList(groupFields);
		
		if (node.get("rollupFrom") != null){
			result.rollupFrom = node.get("rollupFrom").getTextValue();
		}
		
		JsonNode commonSortByNode = node.get("commonSortBy");
		result.commonCriteria = Criteria.parse(commonSortByNode);
		result.sourcesOrder = Order.valueOf(node.get("sourcesOrder").getTextValue());
		
		Iterator<JsonNode> secondaryNode = node.get("secondarySortBys").getElements();
		result.secondaryCriterias = new ArrayList<Criteria>();
		while (secondaryNode.hasNext()){
			JsonNode n = secondaryNode.next();
			result.secondaryCriterias.add(n.isNull() ? null : Criteria.parse(n));
		}
		
  	return result;
		} catch(CoGrouperException e){
			throw new IOException(e);
		}
  }
	
	void toJson(JsonGenerator gen) throws IOException {
    gen.writeStartObject();
    gen.writeArrayFieldStart("sourceSchemas");
    for (Schema schema : sourceSchemas){
    	schema.toJson(gen);
    }
    gen.writeEndArray();
    
    gen.writeArrayFieldStart("groupByFields");
    for(String field : groupByFields){
    	gen.writeString(field);
    }
    gen.writeEndArray();
    
    if (rollupFrom != null){
    	gen.writeFieldName("rollupFrom");
    	gen.writeString(rollupFrom);
    }
    
    gen.writeFieldName("commonSortBy");
    commonCriteria.toJson(gen);
    
    gen.writeStringField("sourcesOrder",sourcesOrder.toString());
    
    gen.writeArrayFieldStart("secondarySortBys");
    for (Criteria c : secondaryCriterias){
    	if (c == null){
    		gen.writeNull();
    	} else {
    		c.toJson(gen);
    	}
    }
    gen.writeEndArray();
    gen.writeEndObject();
  }
	
	@Override
	public String toString(){
		return toString(true);
	}
	
	public String toString(boolean pretty) {
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator gen = FACTORY.createJsonGenerator(writer);
      if (pretty) gen.useDefaultPrettyPrinter();
      toJson(gen);
      gen.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
	}
	
	
	/** Parse a schema from the provided string.
   * If named, the schema is added to the names known to this parser. */
  public static CoGrouperConfig parse(String s) throws IOException {
      return parse(FACTORY.createJsonParser(new StringReader(s)));
  }

  private static CoGrouperConfig parse(JsonParser parser) throws IOException {
    try {
      return parse(MAPPER.readTree(parser));
    } catch (JsonParseException e) {
      throw new IOException(e);
    } 
  }
  
  
  public boolean equals(Object a){
  	if (!(a instanceof CoGrouperConfig)){
  		return false;
  	}
  	CoGrouperConfig that = (CoGrouperConfig)a;
  	
  	return (this.getSourcesOrder() ==  that.getSourcesOrder() &&
  			this.getCommonCriteria().equals(that.getCommonCriteria()) && 
  			this.getGroupByFields().equals(that.getGroupByFields()) &&
  			this.getSourceSchemas().equals(that.getSourceSchemas()) &&
  			this.getSecondarySortBys().equals(that.getSecondarySortBys()));
  }
}

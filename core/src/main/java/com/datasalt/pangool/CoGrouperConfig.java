package com.datasalt.pangool;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
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
	
	private Criteria commonSortBy;
	private List<Criteria> secondarySortBys=new ArrayList<Criteria>();
	private Order sourcesOrder;
	
	private List<Schema> sourceSchemas = new ArrayList<Schema>();
	private List<String> groupByFields;
	private String rollupFrom;
	
	private SerializationInfo serInfo;
	
	void setSourceOrder(Order order){
		this.sourcesOrder = order;
	}
	
	public Order getSourcesOrder(){
		return sourcesOrder;
	}
	
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
	
//if(grouperConf.getRollupFrom() != null) {
	//
//			// Check that rollupFrom is contained in groupBy
	//
//			if(!grouperConf.getGroupByFields().contains(grouperConf.getRollupFrom())) {
//				throw new CoGrouperException("Rollup from [" + grouperConf.getRollupFrom() + "] not contained in group by fields "
//				    + grouperConf.getGroupByFields());
//			}
	//
//			// Check that we are using the appropriate Handler
	//
//			if(!(grouperHandler instanceof GroupHandlerWithRollup)) {
//				throw new CoGrouperException("Can't use " + grouperHandler + " with rollup. Please use "
//				    + GroupHandlerWithRollup.class + " instead.");
//			}
//		}
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
		result.commonSortBy = Criteria.parse(commonSortByNode);
		result.sourcesOrder = Order.valueOf(node.get("sourcesOrder").getTextValue());
		
		Iterator<JsonNode> secondaryNode = node.get("secondarySortBys").getElements();
		result.secondarySortBys = new ArrayList<Criteria>();
		while (secondaryNode.hasNext()){
			JsonNode n = secondaryNode.next();
			result.secondarySortBys.add(n.isNull() ? null : Criteria.parse(n));
		}
		
  	return result;
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
    	gen.writeFieldName("rollUpFrom");
    	gen.writeString(rollupFrom);
    }
    
    gen.writeFieldName("commonSortBy");
    commonSortBy.toJson(gen);
    
    gen.writeStringField("sourcesOrder",sourcesOrder.toString());
    
    gen.writeArrayFieldStart("secondarySortBys");
    for (Criteria c : secondarySortBys){
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
  	
  	if (this.getSourcesOrder() !=  that.getSourcesOrder()){
  		return false;
  	}
  	
  	if (!this.getCommonSortBy().equals(that.getCommonSortBy())){
  		return false;
  	}
  	
  	if (!this.getGroupByFields().equals(that.getGroupByFields())){
  		return false;
  	}
  	
  	if (!this.getSourceSchemas().equals(that.getSourceSchemas())){
  		return false;
  	}
  	
  	if (!this.getSecondarySortBys().equals(that.getSecondarySortBys())){
  		return false;
  	}
  	
  	return true;
  }
  
	
}

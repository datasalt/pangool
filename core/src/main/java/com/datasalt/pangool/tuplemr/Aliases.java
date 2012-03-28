package com.datasalt.pangool.tuplemr;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;

public class Aliases {
	private Map<String,String> aliases=new HashMap<String,String>();
	public Aliases(Map<String,String> aliases){
		this.aliases = aliases;
	}
	
	public Aliases(){	}
  
	public Aliases addAlias(String alias,String reference){
		if (aliases.get(alias) == null){
			aliases.put(alias,reference);
		} else {
			throw new IllegalArgumentException("Alias '" + alias + "' already exists");
		}
		return this;
	}
	
	
	public Map<String,String> getAliases(){
		return aliases;
	}
	
	void toJson(JsonGenerator gen) throws IOException {
		gen.writeStartObject();
		for (Map.Entry<String,String> entry : aliases.entrySet()){
			String alias = entry.getKey();
			String ref = entry.getValue();
			gen.writeStringField(alias, ref);
		}
		gen.writeEndObject();
	}
	
	public static Aliases parse(JsonNode node) throws IOException {
		Iterator<String> aliases = node.getFieldNames();
		Map<String,String> map = new HashMap<String,String>();
		while (aliases.hasNext()){
			String alias = aliases.next();
			String ref = node.get(alias).getTextValue();
			map.put(alias, ref);
		}
		return new Aliases(map);
	}
	
}

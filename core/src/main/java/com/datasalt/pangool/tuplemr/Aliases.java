/**
 * Copyright [2012] [Datasalt Systems S.L.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasalt.pangool.tuplemr;

import java.io.IOException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
/**
 * 
 * Builder that contains pairs of (alias, referenced_item). Used in any place that an item
 * must be referenced with a different name. 
 * Used in {@link TupleMRConfigBuilder#setFieldAliases(String, Aliases)}
 *
 */
public class Aliases {
	private Map<String,String> aliases=new HashMap<String,String>();
	public Aliases(Map<String,String> aliases){
		this.aliases = aliases;
	}
	
	public Aliases(){	}
  
	/**
	 * Adds an alias
	 * @param alias
	 * @param reference
	 * 
	 */
	public Aliases add(String alias,String reference){
		if (aliases.get(alias) == null){
			aliases.put(alias,reference);
		} else {
			throw new IllegalArgumentException("Alias '" + alias + "' already exists");
		}

		return this;
	}
	
	/**
	 * 
	 * @return The map containing alias as keys,and referenced items as values.
	 */
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

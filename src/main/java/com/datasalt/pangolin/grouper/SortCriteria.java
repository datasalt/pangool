/**
 * Copyright [2011] [Datasalt Systems S.L.]
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

package com.datasalt.pangolin.grouper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

/**
 * 
 * SortCriteria specifies how the {@link Tuple} fields will be sorted by {@link TupleSortComparator}.
 * Basically it's a list of fields with a sort descriptor (scending or descending order).For example: "name asc,age desc"
 * The sort criteria needs to match the {@link Schema} fields ordering, so in any case the {@link SortCriteria} must be a prefix from {@link Schema} 
 * 
 * @author epalace
 *
 */
public class SortCriteria  {
	
	public static final String CONF_SORT_CRITERIA = "datasalt.grouper.sort.criteria";
	
	public static enum SortOrder {
		ASCENDING, 
		DESCENDING
	}
	
	private SortCriteria(Map<String,SortOrder> fields,String[] namesOrdered){
		this.fields = fields;
		this.namesOrdered = namesOrdered;
	}
	private Map<String,SortOrder> fields;
	private String[] namesOrdered;
	
	public SortOrder getSortByFieldName(String name){
		return fields.get(name);
	}
	
	public String[] getFieldNames(){
		return namesOrdered;
	}
	
	public static SortCriteria parse(Configuration conf) throws GrouperException{
		return parse(conf.get(CONF_SORT_CRITERIA));
	}
	
	public static SortCriteria parse(String sortCriteria) throws GrouperException {
		Map<String,SortOrder> fields = new HashMap<String,SortOrder>();
		List<String> namesOrdered = new ArrayList<String>();
		String[] tokens = sortCriteria.split(",");
		for (String token : tokens){
			String[] nameSort = token.trim().split("\\s+");
			if (nameSort.length != 2){
				throw new GrouperException("Invalid sortCriteria format : " + sortCriteria);
			}
			String name = nameSort[0].toLowerCase();
			if (fields.containsKey(name)){
				throw new GrouperException("Invalid sortCriteria .Repeated field " + name);
			}
			
			if ("ASC".equals(nameSort[1].toUpperCase())){
				fields.put(name,SortOrder.ASCENDING);
			} else if("DESC".equals(nameSort[1].toUpperCase())){
				fields.put(name,SortOrder.DESCENDING);
			} else {
				throw new GrouperException ("Invalid sortingCriteria " + nameSort[1] + " in " + sortCriteria);
			}
			namesOrdered.add(name);
			
		}
		String[] array = new String[namesOrdered.size()];
		namesOrdered.toArray(array);
		return new SortCriteria(fields,array);
	}
	
	@Override
	public String toString(){
		if (namesOrdered == null || namesOrdered.length == 0){
			return "";
		}
		StringBuilder b = new StringBuilder();
		String name = namesOrdered[0];
		SortOrder sort = fields.get(name);
		b.append(name).append(" ").append(sort);
		for (int i=1 ; i < namesOrdered.length; i++){
			name = namesOrdered[i];
			sort = fields.get(name);
			b.append(",").append(name).append(" ").append(sort);
		}
		return b.toString();
	}
}

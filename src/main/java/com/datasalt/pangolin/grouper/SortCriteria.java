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
import com.datasalt.pangolin.grouper.io.tuple.SortComparator;
import com.datasalt.pangolin.grouper.io.tuple.ITuple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;

/**
 * 
 * SortCriteria specifies how the {@link ITuple} fields will be sorted by {@link SortComparator}.
 * Basically it contains a list of fields with a sort descriptor {@link SortOrder} (ascending or descending order).
 * For example: "name asc,age desc"
 * The sort criteria needs to match the {@link FieldsDescription} fields ordering, so in any case the {@link SortCriteria} must 
 * be a prefix from {@link FieldsDescription} 
 * TODO : this can change in the future
 * 
 * 
 * @author eric
 *
 */
public class SortCriteria  {
	
	public static final String CONF_SORT_CRITERIA = "datasalt.grouper.sort.criteria";
	
	private SortCriteria(){
		
	}
	
	public Map<String, SortElement> getSortElementsByName() {
  	return sortElementsByName;
  }

	public void setSortElementsByName(Map<String, SortElement> sortElementsByName) {
  	this.sortElementsByName = sortElementsByName;
  }

	public void setSortElements(SortElement[] sortElements) {
  	this.sortElements = sortElements;
  }

	public static class SortElement {
		
		public SortElement(String name,SortOrder sortOrder,Class<? extends RawComparator> comparator){
			this.fieldName = name;
			this.sortOrder = sortOrder;
			this.comparator = comparator;
		}
		
		
		
		public String getFieldName() {
    	return fieldName;
    }

		public void setFieldName(String fieldName) {
    	this.fieldName = fieldName;
    }

		public void setSortOrder(SortOrder sortOrder) {
    	this.sortOrder = sortOrder;
    }

		public void setComparator(Class<? extends RawComparator> comparator) {
    	this.comparator = comparator;
    }

		private String fieldName;
		private SortOrder sortOrder;
		private Class<? extends RawComparator> comparator;
		public String getName(){
			return fieldName;
		}
		
		public SortOrder getSortOrder(){
			return sortOrder;
		}
		
		public Class<? extends RawComparator> getComparator(){
			return comparator;
		}
	}
	
	public static enum SortOrder {
		ASCENDING("asc"), 
		DESCENDING("desc");
		
		private String abr;
		private SortOrder(String abr){
			this.abr=abr;
		}
		public String getAbreviation(){
			return abr;
		}
		
	}
	
	SortCriteria(SortElement[] sortElements){
		this.sortElements = sortElements;
		for (SortElement sortElement : sortElements){
			this.sortElementsByName.put(sortElement.fieldName, sortElement);
		}
		
	}

	private SortElement[] sortElements;
	private Map<String,SortElement> sortElementsByName=new HashMap<String,SortElement>();
	
	public SortElement getSortElementByFieldName(String name){
		return sortElementsByName.get(name.toLowerCase());
	}
	
	public SortElement[] getSortElements(){
		return sortElements;
	}
	
	
	public static SortCriteria parse(Configuration conf) throws GrouperException{
		return parse(conf.get(CONF_SORT_CRITERIA));
	}
	
	public static SortCriteria parse(String sortCriteria) throws GrouperException {
		List<SortElement> sortElements = new ArrayList<SortElement>();
		List<String> fields = new ArrayList<String>();
		String[] tokens = sortCriteria.split(",");
		for (String token : tokens){
			
			String[] nameSort = token.trim().split("\\s+");
			if (nameSort.length < 2 || nameSort.length > 4){
				throw new GrouperException("Invalid sortCriteria format : " + sortCriteria);
			}
			String name = nameSort[0].toLowerCase();
			if (fields.contains(name)){
				throw new GrouperException("Invalid sortCriteria .Repeated field " + name);
			}
			fields.add(name);
			int offset=0;
			Class<? extends RawComparator> comparator = null;
			try{
			if ("using".equals(nameSort[1].toLowerCase())){
				comparator = (Class<? extends RawComparator<?>>)Class.forName(nameSort[2]);
				offset=2;
			}
			} catch(ClassNotFoundException e){
				throw new GrouperException("Class not found : " + nameSort[2],e);
			}
			
			SortOrder sortOrder;
			if ("ASC".equals(nameSort[1+offset].toUpperCase())){
				sortOrder = SortOrder.ASCENDING;
			} else if("DESC".equals(nameSort[1+offset].toUpperCase())){
				sortOrder = SortOrder.DESCENDING;
			} else {
				throw new GrouperException ("Invalid sortingCriteria " + nameSort[1] + " in " + sortCriteria);
			}
			
			SortElement sortElement = new SortElement(name,sortOrder,comparator);
			sortElements.add(sortElement);
		}

		SortElement[] array = new SortElement[sortElements.size()];
		sortElements.toArray(array);
		return new SortCriteria(array);
	}
	
	@Override
	public String toString(){
		
		StringBuilder b = new StringBuilder();
		
		for (int i=0 ; i < sortElements.length; i++){
			if (i!=0){
			  b.append(",");
			}
			SortElement sortElement = sortElements[i];
			b.append(sortElement.getName());
			Class<?> comparator = sortElement.getComparator();
			if (comparator != null){
				b.append(" using ").append(comparator.getName());
			}
			b.append(" ").append(sortElement.getSortOrder().getAbreviation());
		}
		return b.toString();
	}
	
	public static void setInConfig(SortCriteria criteria,Configuration conf){
		conf.set(CONF_SORT_CRITERIA, criteria.toString());
	}
	
}

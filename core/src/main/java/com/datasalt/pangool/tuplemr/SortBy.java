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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.RawComparator;
import org.codehaus.jackson.map.ObjectMapper;

import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.Criteria.SortElement;

/**
 * SortBy is a convenience builder used by {@link TupleMRConfig} , similar to {@link Criteria}.
 * The main difference is that {@link SortBy} is mutable using the concatenation pattern and 
 * allows to specify sourceOrder. 
 * 
 * The SortBy instances are converted to immutable Criteria objects by {@link TupleMRConfig}.
 */
public class SortBy {
	
	private Order sourceOrder;
	private Integer sourceOrderIndex;
	private List<SortElement> elements = new ArrayList<SortElement>();
	
	public SortBy(List<SortElement> elements){
		this.elements = elements;
	}
	
	public SortBy(){
	}
	
	/**
	 * Adds a new field to order by and its specified order.
	 * 
	 * @param name Field's name
	 * @param order Field's order
	 * 
	 * @see Order
	 */
	public SortBy add(String name, Order order){
		failIfFieldNamePresent(name);
		this.elements.add(new SortElement(name,order));
		return this;
	}
	
	/**
	 * Same as {@link #add(String, Order)} but adding the possibility
	 * to specify a custom comparator for that field.
	 * 
	 * @param name Field's name
	 * @param order Field's order
	 * @param comparator Custom comparator instance
	 * 
	 * @see Order
	 */
	public SortBy add(String name, Order order,RawComparator<?> comparator){
		failIfFieldNamePresent(name);
		this.elements.add(new SortElement(name,order,comparator));
		return this;
	}
	/**
	 * This method,unlike the traditional {@link #add} method, adds a symbolic elements to order by.
	 * 
	 * This method only works in a multi-schema scenario, and it specifies that tuples will be sorted by their schema,not by a field's name.
	 * Example : 
	 * b.addIntermediateSchema(schema1);
	 * b.addIntermediateSchema(schema2);
	 * b.setOrderBy(new SortBy().add("user_id",Order.ASC).addSourceOrder(Order.DESC));
	 *  
	 * In the case above, tuples will be first sorted by user_id and then if they compare as equals then tuples from 
	 * schema2 will sort before those from schema1. 
	 * 
	 * This method must be called just once, and it's not allowed in {@link TupleMRConfigBuilder#setSecondaryOrderBy(String, SortBy)
	 *  
	 */
	public SortBy addSourceOrder(Order order){
		if (this.sourceOrderIndex != null){
			throw new IllegalStateException("The schema order is already set");
		}
		this.sourceOrder = order;
		this.sourceOrderIndex = getElements().size();
		return this;
	}
	
	/**
	 * Returns a {@link SortElement} object for every field added to this builder. 
	 * @see SortElement
	 */
	public List<SortElement> getElements(){
		return elements;
	}
	
	/**
	 * Gets the sourceOrder if set.
	 */
	public Order getSourceOrder(){
		return sourceOrder;
	}
	
	/**
	 * Returns the position in the list where sourceOrder was added using {@link #addSourceOrder(Order)}
	 */
	public Integer getSourceOrderIndex(){
		return sourceOrderIndex;
	}
	
	private void failIfFieldNamePresent(String name){
			if (containsFieldName(name)){
				throw new IllegalArgumentException("Sort element with field name '" + name + "' is already present");
			}
	}
	
	/**
	 * True if field was added using {@link #add(String, Order)}
	 */
	public boolean containsFieldName(String field){
		for (SortElement e : elements){
			if (e.getName().equals(field)){
				return true;
			}
		}
		return false;
	}
	
	/**
	 * True if field was added before calling {@link #addSourceOrder(Order)}
	 */
	public boolean containsBeforeSourceOrder(String field){
		if (sourceOrderIndex == null){
			return containsFieldName(field);
		}
		for (int i=0 ; i < sourceOrderIndex ; i++){
			if (elements.get(i).getName().equals(field)){
				return true;
			}
		}
		return false;
	}
	
	public String toString(){
		ObjectMapper mapper = new ObjectMapper();
		
		try {
      return mapper.writeValueAsString(elements);
    } catch(Exception e) {
      throw new RuntimeException(e);
    }
	}
}

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
package com.datasalt.pangool.cogroup.sorting;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.RawComparator;
import org.codehaus.jackson.map.ObjectMapper;

import com.datasalt.pangool.cogroup.sorting.Criteria.Order;
import com.datasalt.pangool.cogroup.sorting.Criteria.SortElement;


public class SortBy {
	
	private Order sourceOrder;
	private Integer sourceOrderIndex;
	
	public SortBy addSourceOrder(Order order){
		if (this.sourceOrderIndex != null){
			throw new IllegalStateException("The schema order is already set");
		}
		this.sourceOrder = order;
		this.sourceOrderIndex = getElements().size();
		return this;
	}
	
	public Order getSourceOrder(){
		return sourceOrder;
	}
	
	public Integer getSourceOrderIndex(){
		return sourceOrderIndex;
	}
	
	private void failIfFieldNamePresent(String name){
			if (containsFieldName(name)){
				throw new IllegalArgumentException("Sort element with field name '" + name + "' is already present");
			}
	}
	
	public boolean containsFieldName(String field){
		for (SortElement e : elements){
			if (e.getName().equals(field)){
				return true;
			}
		}
		return false;
	}
	
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
	
	public SortBy add(String name, Order order){
		failIfFieldNamePresent(name);
		this.elements.add(new SortElement(name,order));
		return this;
	}
	
	public SortBy add(String name, Order order,RawComparator<?> comparator){
		failIfFieldNamePresent(name);
		this.elements.add(new SortElement(name,order,comparator));
		return this;
	}
	
	public String toString(){
		ObjectMapper mapper = new ObjectMapper();
		
		try {
      return mapper.writeValueAsString(elements);
    } catch(Exception e) {
      throw new RuntimeException(e);
    }
	}
	
	private List<SortElement> elements = new ArrayList<SortElement>();
	
	public SortBy(List<SortElement> elements){
		this.elements = elements;
	}
	
	public SortBy(){
	}
	
	public List<SortElement> getElements(){
		return elements;
	}

}

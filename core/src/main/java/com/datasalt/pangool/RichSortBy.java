package com.datasalt.pangool;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;

import com.datasalt.pangool.SortBy.Order;
import com.datasalt.pangool.SortBy.SortElement;

public class RichSortBy {
	
	private Order sourceOrder;
	private Integer sourceOrderIndex;
	
	public RichSortBy addSourceOrder(Order order){
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
	
	public RichSortBy add(String name, Order order){
		this.elements.add(new SortElement(name,order));
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
	
	public RichSortBy(List<SortElement> elements){
		this.elements = elements;
	}
	
	public RichSortBy(){
	}
	
	public List<SortElement> getElements(){
		return elements;
	}

}

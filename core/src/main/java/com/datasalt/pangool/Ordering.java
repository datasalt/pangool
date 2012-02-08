package com.datasalt.pangool;
import java.util.ArrayList;
import java.util.List;


import org.codehaus.jackson.map.ObjectMapper;

import com.datasalt.pangool.SortCriteria.SortOrder;


public class Ordering {

		private List<SortElement> elements = new ArrayList<SortElement>();
	
		public Ordering(List<SortElement> elements){
			this.elements = elements;
		}
		
		public Ordering(){
		}
		
		public List<SortElement> getElements(){
			return elements;
		}
		
		public static class SortElement {
			public String name;
			public SortOrder order;
			public String getName() {
      	return name;
      }
			public void setName(String name) {
      	this.name = name;
      }
			public SortOrder getOrder() {
      	return order;
      }
			public void setOrder(SortOrder order) {
      	this.order = order;
      }
			public SortElement(String name,SortOrder order){this.name =name; this.order = order;}
		}
		
		public Ordering add(String name, SortOrder order){
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

}

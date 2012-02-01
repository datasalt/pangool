package com.datasalt.avrool;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema.Field.Order;
import org.codehaus.jackson.map.ObjectMapper;


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
			public Order order;
			public String getName() {
      	return name;
      }
			public void setName(String name) {
      	this.name = name;
      }
			public Order getOrder() {
      	return order;
      }
			public void setOrder(Order order) {
      	this.order = order;
      }
			public SortElement(String name,Order order){this.name =name; this.order = order;}
		}
		
		public Ordering add(String name, Order order){
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

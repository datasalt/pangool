package com.datasalt.pangool;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.RawComparator;
import org.codehaus.jackson.map.ObjectMapper;


public class Criteria {

	public static enum Order {
		ASC("asc"), DESC("desc");

		private String abr;

		private Order(String abr) {
			this.abr = abr;
		}

		public String getAbreviation() {
			return abr;
		}
	}
	
		private List<SortElement> elements = new ArrayList<SortElement>();
	
		public Criteria(List<SortElement> elements){
			this.elements = Collections.unmodifiableList(elements);
		}
		
		public Criteria(){
		}
		
		public List<SortElement> getElements(){
			return elements;
		}
		
		public static class SortElement {
			private String name;
			private Order order;
			private Class<? extends RawComparator<?>> customComparator;
			
			public Class<? extends RawComparator<?>> getCustomComparator() {
				return customComparator;
			}
			public void setCustomComparator(Class<? extends RawComparator<?>> customComparator) {
				this.customComparator = customComparator;
			}
			public String getName() {
      	return name;
      }
			public void setName(String name) { //TODO remove this setters and make proper Json serialization
      	this.name = name;
      }
			public Order getOrder() {
      	return order;
      }
			public void setOrder(Order order) {
      	this.order = order;
      }
			public SortElement(String name,Order order){this.name =name; this.order = order;}
			public SortElement(String name,Order order,Class<? extends RawComparator<?>> comparator){
				this(name,order); 
				this.customComparator = comparator;
			}
			
			public String toString(){
				//TODO do proper Json serializing
				return name + " " + order + " " +customComparator;
			}
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

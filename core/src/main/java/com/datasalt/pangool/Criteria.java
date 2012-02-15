package com.datasalt.pangool;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.RawComparator;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
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
			
			public Order getOrder() {
      	return order;
      }
			
			@Override
			public boolean equals(Object a){
				if (!(a instanceof SortElement)){
					return false;
				}
				
				SortElement that = (SortElement)a;
				if (this.getCustomComparator() != that.getCustomComparator()){
					return false;
				}
				return this.getName().equals(that.getName()) && this.getOrder().equals(that.getOrder());
			}
			
			public SortElement(String name,Order order){this.name =name; this.order = order;}
			public SortElement(String name,Order order,Class<? extends RawComparator<?>> comparator){
				this(name,order); 
				this.customComparator = comparator;
			}
			
			void toJson(JsonGenerator gen) throws IOException{
				gen.writeStartObject();
				gen.writeStringField("name", name);
				gen.writeStringField("order",order.toString());
				if (customComparator != null){
					gen.writeStringField("comparator", customComparator.getName().toString());
				}
				gen.writeEndObject();
			}
			
			static SortElement parse(JsonNode node) throws IOException {
				
				String name = node.get("name").getTextValue();
				Order order = Order.valueOf(node.get("order").getTextValue());
				if (node.get("comparator") != null){
					Class customComparator;
          try {
	          customComparator = Class.forName(node.get("comparator").getTextValue());
          } catch(ClassNotFoundException e) {
	          throw new IOException(e);
          }
					return new SortElement(name,order,customComparator);
				} else {
					return new SortElement(name, order);
				}
			}
			@Override
			public String toString(){
				//TODO maybe refactor this
				try{
				StringWriter w = new StringWriter();
				JsonGenerator gen =new JsonFactory().createJsonGenerator(w); 
				toJson(gen); 
				gen.flush();
				return w.toString();
				} catch(IOException e){
					throw new RuntimeException(e);
				}
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
		
		void toJson(JsonGenerator gen) throws IOException {
			gen.writeStartArray();
			for (SortElement s : elements){
				s.toJson(gen);
			}
			gen.writeEndArray();
		}
		
		static Criteria parse(JsonNode node) throws IOException {
			Iterator<JsonNode> elements = node.getElements();
			List<SortElement> sorts = new ArrayList<SortElement>();
			while (elements.hasNext()){
				sorts.add(SortElement.parse(elements.next()));
			}
			return new Criteria(sorts);
		}
		
		@Override
		public boolean equals(Object a){
			if (a instanceof Criteria){
				return getElements().equals(((Criteria)a).getElements());
			} else {
				return false;
			}
		}

}

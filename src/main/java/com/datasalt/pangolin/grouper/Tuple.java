package com.datasalt.pangolin.grouper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.WritableComparable;
import org.apache.thrift.TBase;

/**
 * TODO
 * @author epalace
 *
 */
public class Tuple implements WritableComparable{

	public static class Schema {
		private List<String> fieldNames;
		private List<Class> fieldTypes;
		
		private Map<String,Integer> indexByFieldName = new HashMap<String,Integer>();
		
		public Schema(List<String> fieldNames,List<Class> fieldTypes){
			if (fieldNames.size() != fieldTypes.size()){
				throw new RuntimeException("Field names size ("+ fieldNames.size() + ") doesn't match fieldTypes  (" + fieldTypes.size());
			}
			this.fieldNames = fieldNames;
			this.fieldTypes = fieldTypes;
		}
		
		public List<String> getFieldNames(){
			return fieldNames;
		}
		
		public List<Class> getFieldTypes(){
			return fieldTypes;
		}
		
		public String getFieldName(int index){
			return fieldNames.get(index);
		}
		
		public Class getFieldType(int index){
			return fieldTypes.get(index);
		}
		
		public int getNumFields(){
			return fieldNames.size();
		}
	}
	
	public static class Prefix {
		
	}
	
	
	private Comparable[] fields;
	private Schema schema;
	public Tuple(){	}
	
	public void setSchema(Schema schema){
		this.schema = schema;
		this.fields = new Comparable[schema.getNumFields()];
	}
	
	public void setField(String fieldName,Object value){
		//TODO
	}
	
	public void setField(int index,Comparable value){
		this.fields[index] = value;
	}
	
	
	@Override
  public void readFields(DataInput input) throws IOException {
		for(int numField = 0 ; numField < schema.getNumFields(); numField++){
			Class fieldType = schema.getFieldType(numField);
			if (fieldType == Integer.class){
			 setField(numField,input.readInt());
			} else if(fieldType == Long.class){
				setField(numField,input.readLong());
			} else if(fieldType == Double.class){
				setField(numField,input.readDouble());
			} else if(fieldType == Float.class){
				setField(numField,input.readFloat());
			} else if(fieldType == String.class){
				setField(numField,input.readUTF()); //TODO use Text ?
			} else if(fieldType == TBase.class){ //TODO improve this .
				//TODO ..
				
			}else {
				//TODO
				throw new RuntimeException("Not implemented fieldType :  " + fieldType);
			}
		}
  }

	@Override
  public void write(DataOutput output) throws IOException {
	  for (int numField = 0 ; numField < schema.getNumFields(); numField++){
	  	Class fieldType = schema.getFieldType(numField);
	  	if (fieldType == Integer.class){
	  		output.writeInt((Integer)fields[numField]);
	  	} else if (fieldType == Long.class){
	  		output.writeLong((Long)fields[numField]);
	  	} else if (fieldType == Double.class){
	  		output.writeDouble((Double) fields[numField]);
	  	} else if(fieldType == Float.class){
	  		output.writeFloat((Float)fields[numField]);
	  	} else if(fieldType == String.class){
	  		output.writeUTF((String)fields[numField]);
	  	} else {
	  		throw new RuntimeException("Not implemented fieldType : "+ fieldType); //TODO output correct exception
	  	}
	  }
  }

	public void set(Tuple tuple){ 
		this.schema = tuple.schema;
		if (fields == null || fields.length != schema.getNumFields()){
			fields = new Comparable[schema.getNumFields()];
		}
		for (int i = 0 ; i < schema.getNumFields(); i++){
			fields[i] = tuple.fields[i];
		}
	}
	
	/**
	 *  returns the level where they mismatch. 
	 *  Returns 0 if equals
	 * @param tuple1
	 * @param tuple2
	 * @param levels
	 * @return
	 */
	public static int compareLevels(Tuple tuple1,Tuple tuple2,int levels){
		for (int i = 0 ; i < levels; i++){
			int comparison =tuple1.fields[i].compareTo((tuple2.fields[i])); 
			if (comparison != 0){
				return i;
			}
		}
		return 0;
	}
	
	
	@Override
  public int compareTo(Object that) {
	  // TODO Auto-generated method stub
	  return 0;
  }

}

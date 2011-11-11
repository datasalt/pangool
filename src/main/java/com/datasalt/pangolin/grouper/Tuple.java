package com.datasalt.pangolin.grouper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.thrift.TBase;

/**
 * TODO
 * @author epalace
 *
 */
public class Tuple implements WritableComparable,Configurable{

	
	
	
	public static class Prefix {
		
	}
	
	
	private Comparable[] fields;
	private Schema schema;
	public Tuple(){	}
	
	public void setSchema(Schema schema){
		this.schema = schema;
		//TODO this should erase previous state ? 
		if (this.fields.length != schema.getNumFields()){
			this.fields = new Comparable[schema.getNumFields()];
		}
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
	
	
	
	public int partialHashCode(int[] fieldsIndexes){
		int result = 0 ; 
		for ( int fieldIndex : fieldsIndexes){
			result = result*31 + fields[fieldIndex].hashCode();
		}
		
		return result & Integer.MAX_VALUE;
	}

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setConf(Configuration conf) {
		if (conf != null){
			String schemaStr = conf.get(Grouper.CONF_SCHEMA);
			this.schema = Schema.parse(schemaStr);
		}
		
	}
	

}

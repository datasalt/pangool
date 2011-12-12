package com.datasalt.pangolin.grouper.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.thrift.TBase;

import com.datasalt.pangolin.grouper.FieldsDescription;
import com.datasalt.pangolin.grouper.GrouperException;
import com.datasalt.pangolin.grouper.io.TupleImpl.InvalidFieldException;
import com.datasalt.pangolin.io.Serialization;

/**
 * 
 * 
 * 
 * @author epalace
 *
 */
public class DoubleBufferedTuple implements Tuple {
	
	private FieldsDescription schema;
	private Configuration conf;	
	private TupleImpl previousTuple,currentTuple;
	
	
	private DoubleBufferedTuple(){
		
	}
	
	public DoubleBufferedTuple(FieldsDescription schema){
		this.schema = schema;
		currentTuple = new TupleImpl(schema);
		
		//previousTuple = new TupleImpl(schema);
		
	}

	@Override
  public void write(DataOutput out) throws IOException {
	  currentTuple.write(out);
	  
  }

	@Override
  public void readFields(DataInput in) throws IOException {
	  //swapping double buffering
		if (previousTuple == null){
			previousTuple = ReflectionUtils.newInstance(TupleImpl.class, conf);
		}
		TupleImpl tmpTuple = previousTuple;
		previousTuple = currentTuple;
		currentTuple = tmpTuple;
		
		currentTuple.readFields(in);
  }

	public TupleImpl getPreviousTuple(){
		return previousTuple;
	}
	
	
	@Override
  public int compareTo(Tuple anotherTuple) {
	  return currentTuple.compareTo(anotherTuple);
  }

	@Override
  public void setConf(Configuration conf) {
		if (conf != null){
			this.conf = conf;
			try {
				FieldsDescription schema =FieldsDescription.parse(this.conf);
				if (schema != null){
					this.schema = schema;
				}
				
				if (previousTuple != null){
					previousTuple.setConf(conf);
				}
				
				if (currentTuple != null){
					currentTuple.setConf(conf);
				}
      } catch(GrouperException e) {
	      throw new RuntimeException(e);
      }
		}
	  
  }

	@Override
  public Configuration getConf() {
	 return conf;
  }

	@Override
  public FieldsDescription getSchema() {
	  return schema;
  }

	@Override
  public int partialHashCode(String[] fields) throws InvalidFieldException {
	  return currentTuple.partialHashCode(fields);
  }

	@Override
  public int getInt(String fieldName) throws InvalidFieldException {
	  return currentTuple.getInt(fieldName);
  }

	@Override
  public long getLong(String fieldName) throws InvalidFieldException {
	  return currentTuple.getLong(fieldName);
  }

	@Override
  public float getFloat(String fieldName) throws InvalidFieldException {
	  return currentTuple.getFloat(fieldName);
  }

	@Override
  public double getDouble(String fieldName) throws InvalidFieldException {
	  return currentTuple.getDouble(fieldName);
  }

	@Override
  public String getString(String fieldName) throws InvalidFieldException {
	  return currentTuple.getString(fieldName);
  }

	@Override
  public Object getObject(String fieldName) throws InvalidFieldException {
	  return currentTuple.getObject(fieldName);
  }

	@Override
  public Enum<? extends Enum<?>> getEnum(String fieldName) throws InvalidFieldException {
	  return getEnum(fieldName);
  }

	@Override
  public void setEnum(String fieldName, Enum<? extends Enum<?>> value) throws InvalidFieldException {
	  currentTuple.setEnum(fieldName,value);
	  
  }

	@Override
  public void setInt(String fieldName, int value) throws InvalidFieldException {
	  currentTuple.setInt(fieldName,value);
	  
  }

	@Override
  public void setString(String fieldName, String value) throws InvalidFieldException {
	  currentTuple.setString(fieldName,value);
	  
  }

	@Override
  public void setLong(String fieldName, long value) throws InvalidFieldException {
	  currentTuple.setLong(fieldName,value);
	  
  }

	@Override
  public void setFloat(String fieldName, float value) throws InvalidFieldException {
	  currentTuple.setFloat(fieldName,value);
	  
  }

	@Override
  public void setDouble(String fieldName, double value) throws InvalidFieldException {
	  currentTuple.setDouble(fieldName,value);
	  
  }

	@Override
  public void setBoolean(String fieldName, boolean value) throws InvalidFieldException {
	  currentTuple.setBoolean(fieldName,value);
	  
  }

	@Override
  public void setObject(String fieldName, Object object) throws InvalidFieldException {
	  currentTuple.setObject(fieldName, object);
	  
  }

	@Override
  public void setThriftObject(String fieldName, TBase value) throws InvalidFieldException {
	  currentTuple.setThriftObject(fieldName, value);
  }

	@Override
	public String toString(){
		return currentTuple.toString();
	}
	
}

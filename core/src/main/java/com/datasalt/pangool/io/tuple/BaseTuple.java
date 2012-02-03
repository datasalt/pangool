package com.datasalt.pangool.io.tuple;

import org.apache.hadoop.io.Text;

import com.datasalt.pangool.Schema;

public abstract class BaseTuple implements ITuple{

	
	@Override
  public Integer getInt(int pos) {
	  return (Integer)get(pos);
  }

	@Override
  public Integer getInt(String field) {
	  return (Integer)get(field);
  }

	@Override
  public Long getLong(int pos) {
	  return (Long)get(pos);
  }

	@Override
  public Long getLong(String field) {
	  return (Long)get(field);
  }

	@Override
  public Float getFloat(int pos) {
	  return (Float)get(pos);
  }

	@Override
  public Float getFloat(String field) {
	  return (Float)get(field);
  }

	@Override
  public Double getDouble(int pos) {
		return (Double)get(pos);
  }

	@Override
  public Double getDouble(String field) {
	  return (Double)get(field);
  }

	@Override
  public Text getString(int pos) {
	  return (Text)get(pos);
  }

	@Override
  public Text getString(String field) {
	  return (Text)get(field);
  }

	@Override
  public Enum<? extends Enum<?>> getEnum(int pos) {
	  return (Enum<? extends Enum<?>>) get(pos);
  }

	@Override
  public Enum<? extends Enum<?>> getEnum(String field) {
		return (Enum<? extends Enum<?>>) get(field);
  }

	@Override
  public void setEnum(int pos, Enum<? extends Enum<?>> value) {
	  set(pos,value);
	  
  }

	@Override
  public void setEnum(String field, Enum<? extends Enum<?>> value) {
		set(field,value);
	  
  }

	@Override
  public void setInt(int pos, int value) {
		set(pos,value);
	  
  }

	@Override
  public void setInt(String field, int value) {
		set(field,value);
	  
  }

	@Override
  public void setString(int pos, Text value) {
		set(pos,value);
	  
  }

	@Override
  public void setString(String field, Text value) {
		set(field,value);
	  
  }

	@Override
  public void setString(int pos, String value) {
		set(pos,value);
	  
  }

	@Override
  public void setString(String field, String value) {
		set(field,value);
	  
  }

	@Override
  public void setLong(int pos, long value) {
		set(pos,value);
	  
  }

	@Override
  public void setLong(String field, long value) {
		set(field,value);
	  
  }

	@Override
  public void setFloat(int pos, float value) {
		set(pos,value);
	  
  }

	@Override
  public void setFloat(String field, float value) {
		set(field,value);
	  
  }

	@Override
  public void setDouble(int pos, double value) {
		set(pos,value);
	  
  }

	@Override
  public void setDouble(String field, double value) {
	  set(field,value);
	  
  }

	@Override
  public void setBoolean(int pos, boolean value) {
		set(pos,value);
	  
  }

	@Override
  public void setBoolean(String field, boolean value) {
	  set(field,value);
	  
  }

	@Override
  public <T> void setObject(Class<T> valueType, int pos, T value) {
		set(pos,value);
	  
  }

	@Override
  public <T> void setObject(Class<T> valueType, String field, T value) {
	  set(field,value);
	  
  }

	

}

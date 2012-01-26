package com.datasalt.avrool.io.tuple;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.datasalt.avrool.io.tuple.ITuple;
import com.datasalt.avrool.io.tuple.Tuple;

public class DoubleBufferedTuple implements ITuple, ITupleInternal{

  private ITuple currentTuple;
  private ITuple previousTuple;
  
  public DoubleBufferedTuple(ITuple tuple){
  	this.currentTuple = tuple;
		this.previousTuple = new Tuple();
  }
	
	public DoubleBufferedTuple(){
		this.currentTuple = new Tuple();
		this.previousTuple = new Tuple();
	}
			
	public void setContainedTuple(ITuple tuple){
		this.currentTuple = tuple;
	}
	
	public void swapInstances() throws IOException {
		ITuple tmpTuple = previousTuple;
		previousTuple = currentTuple;
		currentTuple = tmpTuple;
	}
		
	public ITuple getPreviousTuple() {
		return previousTuple;
	}
		
	@Override
	public void clear() {
		currentTuple.clear();
	}

	@Override
	public boolean containsKey(Object key) {
		return currentTuple.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return currentTuple.containsValue(value);
	}

	@Override
	public Set<java.util.Map.Entry<String, Object>> entrySet() {
		return currentTuple.entrySet();
	}

	@Override
	public Object get(Object key) {
		return currentTuple.get(key);
	}

	@Override
	public boolean isEmpty() {
		return currentTuple.isEmpty();
	}

	@Override
	public Set<String> keySet() {
		return currentTuple.keySet();
	}

	@Override
	public Object put(String key, Object value) {
		return currentTuple.put(key, value);
	}

	@Override
	public void putAll(Map<? extends String, ? extends Object> arg0) {
		currentTuple.putAll(arg0);
	}

	@Override
	public Object remove(Object arg0) {
		return currentTuple.remove(arg0);
	}

	@Override
	public Collection<Object> values() {
		return currentTuple.values();
	}

	@Override
	public int compareTo(ITuple arg0) {
		return currentTuple.compareTo(arg0);
	}

	@Override
	public int partialHashCode(String[] fields) {
		return currentTuple.partialHashCode(fields);
	}

	@Override
	public int size() {
		return currentTuple.size();
	}

	@Override
	public Integer getInt(String fieldName) {
		return currentTuple.getInt(fieldName);
	}

	@Override
	public Long getLong(String fieldName) {
		return currentTuple.getLong(fieldName);
	}

	@Override
	public Float getFloat(String fieldName) {
		return currentTuple.getFloat(fieldName);
	}

	@Override
	public Double getDouble(String fieldName) {
		return currentTuple.getDouble(fieldName);
	}

	@Override
	public String getString(String fieldName) {
		return currentTuple.getString(fieldName);
	}

	@Override
	public Object getObject(String fieldName) {
		return currentTuple.getObject(fieldName);
	}

	@Override
	public <T> T getObject(Class<T> clazz, String fieldName) {
		return currentTuple.getObject(clazz,fieldName);
	}

	@Override
	public Enum<? extends Enum<?>> getEnum(String fieldName) {
		return currentTuple.getEnum(fieldName);
	}

	@Override
	public void setEnum(String fieldName, Enum<? extends Enum<?>> value) {
		currentTuple.setEnum(fieldName,value);
	}

	@Override
	public void setInt(String fieldName, int value) {
		currentTuple.setInt(fieldName,value);
	}

	@Override
	public void setString(String fieldName, String value) {
		currentTuple.setString(fieldName,value);
	}

	@Override
	public void setLong(String fieldName, long value) {
		currentTuple.setLong(fieldName,value);
		
	}

	@Override
	public void setFloat(String fieldName, float value) {
		currentTuple.setFloat(fieldName,value);
		
	}

	@Override
	public void setDouble(String fieldName, double value) {
		currentTuple.setDouble(fieldName,value);
	}

	@Override
	public void setBoolean(String fieldName, boolean value) {
		currentTuple.setBoolean(fieldName,value);
	}

	@Override
	public void setObject(String fieldName, Object object) {
		currentTuple.setObject(fieldName,object);
	}

	@Override
	public <T> void setObject(Class<T> valueType, String fieldName, T value) {
		currentTuple.setObject(valueType,fieldName,value);
	}

	@Override
	public String toString(){
		return currentTuple.toString();
	}
	
	@Override
	public boolean equals(Object that){
		return currentTuple.equals(that);
	}

	@Override
  public String toString(Collection<String> fields) {
	  return currentTuple.toString(fields);
  }
}

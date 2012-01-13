package com.datasalt.pangool.io.tuple;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.datasalt.pangolin.grouper.Schema;
import com.datasalt.pangolin.grouper.io.tuple.BaseTuple;
import com.datasalt.pangolin.grouper.io.tuple.ITuple;

public class SourcedTuple implements ISourcedTuple{

  private ITuple containedTuple;
	private int sourceId;
	
	public SourcedTuple(){
		this.containedTuple = new BaseTuple();
	}
	
	public SourcedTuple(ITuple containedTuple){
		this.containedTuple = containedTuple;
	}
		
	@Override
	public void setContainedTuple(ITuple tuple){
		this.containedTuple = tuple;
	}
	

	
	@Override
	public int getSource(){
		return sourceId;
	}
	
	@Override
	public void setSource(int sourceId){
		this.sourceId = sourceId;
	}
	
	@Override
	public void clear() {
		containedTuple.clear();
	}

	@Override
	public boolean containsKey(Object key) {
		return containedTuple.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return containedTuple.containsValue(value);
	}

	@Override
	public Set<java.util.Map.Entry<String, Object>> entrySet() {
		return containedTuple.entrySet();
	}

	@Override
	public Object get(Object key) {
		return containedTuple.get(key);
	}

	@Override
	public boolean isEmpty() {
		return containedTuple.isEmpty();
	}

	@Override
	public Set<String> keySet() {
		return containedTuple.keySet();
	}

	@Override
	public Object put(String key, Object value) {
		return containedTuple.put(key, value);
	}

	@Override
	public void putAll(Map<? extends String, ? extends Object> arg0) {
		containedTuple.putAll(arg0);
	}

	@Override
	public Object remove(Object arg0) {
		return containedTuple.remove(arg0);
	}

	@Override
	public Collection<Object> values() {
		return containedTuple.values();
	}

	@Override
	public int compareTo(ITuple arg0) {
		return containedTuple.compareTo(arg0);
	}

	@Override
	public int partialHashCode(String[] fields) {
		return containedTuple.partialHashCode(fields);
	}

	@Override
	public int size() {
		return containedTuple.size();
	}

	@Override
	public Integer getInt(String fieldName) {
		return containedTuple.getInt(fieldName);
	}

	@Override
	public Long getLong(String fieldName) {
		return containedTuple.getLong(fieldName);
	}

	@Override
	public Float getFloat(String fieldName) {
		return containedTuple.getFloat(fieldName);
	}

	@Override
	public Double getDouble(String fieldName) {
		return containedTuple.getDouble(fieldName);
	}

	@Override
	public String getString(String fieldName) {
		return containedTuple.getString(fieldName);
	}

	@Override
	public Object getObject(String fieldName) {
		return containedTuple.getObject(fieldName);
	}

	@Override
	public <T> T getObject(Class<T> clazz, String fieldName) {
		return containedTuple.getObject(clazz,fieldName);
	}

	@Override
	public Enum<? extends Enum<?>> getEnum(String fieldName) {
		return containedTuple.getEnum(fieldName);
	}

	@Override
	public void setEnum(String fieldName, Enum<? extends Enum<?>> value) {
		containedTuple.setEnum(fieldName,value);
	}

	@Override
	public void setInt(String fieldName, int value) {
		containedTuple.setInt(fieldName,value);
	}

	@Override
	public void setString(String fieldName, String value) {
		containedTuple.setString(fieldName,value);
	}

	@Override
	public void setLong(String fieldName, long value) {
		containedTuple.setLong(fieldName,value);
		
	}

	@Override
	public void setFloat(String fieldName, float value) {
		containedTuple.setFloat(fieldName,value);
		
	}

	@Override
	public void setDouble(String fieldName, double value) {
		containedTuple.setDouble(fieldName,value);
	}

	@Override
	public void setBoolean(String fieldName, boolean value) {
		containedTuple.setBoolean(fieldName,value);
	}

	@Override
	public void setObject(String fieldName, Object object) {
		containedTuple.setObject(fieldName,object);
	}

	@Override
	public <T> void setObject(Class<T> valueType, String fieldName, T value) {
		containedTuple.setObject(valueType,fieldName,value);
	}

	@Override
	public String toString(Schema schema, int minFieldIndex, int maxFieldIndex) {
		return containedTuple.toString(schema,minFieldIndex,maxFieldIndex);
	}
	
	@Override
	public String toString(){
		return "source:" + sourceId + "=>" +containedTuple.toString();
	}
	
	@Override
	public boolean equals(Object that){
		if (that == null){
			return false;
		} else if (that instanceof SourcedTuple){
			return sourceId == ((SourcedTuple)that).sourceId 
					&& this.containedTuple.equals(((SourcedTuple)that).containedTuple);
		} else {
			return false;
		}
	}

	@Override
	public ITuple getContainedTuple() {
		return containedTuple;
	}
}

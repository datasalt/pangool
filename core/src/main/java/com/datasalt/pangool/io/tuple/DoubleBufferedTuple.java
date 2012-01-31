package com.datasalt.pangool.io.tuple;

import java.io.IOException;

public class DoubleBufferedTuple implements ITuple, ITupleInternal{

  private ITuple currentTuple;
  private ITuple previousTuple;
  
  public DoubleBufferedTuple(ITuple tuple, ITuple oTuple) {
  	this.currentTuple = tuple;
  	this.previousTuple = oTuple;
  }
  
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
	public int compareTo(ITuple arg0) {
		return currentTuple.compareTo(arg0);
	}

	@Override
	public int partialHashCode(int nFields) {
		return currentTuple.partialHashCode(nFields);
	}

	@Override
	public int size() {
		return currentTuple.size();
	}

	@Override
	public Integer getInt(int pos) {
		return currentTuple.getInt(pos);
	}

	@Override
	public Long getLong(int pos) {
		return currentTuple.getLong(pos);
	}

	@Override
	public Float getFloat(int pos) {
		return currentTuple.getFloat(pos);
	}

	@Override
	public Double getDouble(int pos) {
		return currentTuple.getDouble(pos);
	}

	@Override
	public byte[] getString(int pos) {
		return currentTuple.getString(pos);
	}

	@Override
	public Object getObject(int pos) {
		return currentTuple.getObject(pos);
	}

	@Override
	public <T> T getObject(Class<T> clazz, int pos) {
		return currentTuple.getObject(clazz,pos);
	}

	@Override
	public Enum<? extends Enum<?>> getEnum(int pos) {
		return currentTuple.getEnum(pos);
	}

	@Override
	public void setEnum(int pos, Enum<? extends Enum<?>> value) {
		currentTuple.setEnum(pos,value);
	}

	@Override
	public void setInt(int pos, int value) {
		currentTuple.setInt(pos,value);
	}

	@Override
	public void setString(int pos, byte[] value) {
		currentTuple.setString(pos,value);
	}

	@Override
	public void setLong(int pos, long value) {
		currentTuple.setLong(pos,value);
	}

	@Override
	public void setFloat(int pos, float value) {
		currentTuple.setFloat(pos,value);
		
	}

	@Override
	public void setDouble(int pos, double value) {
		currentTuple.setDouble(pos,value);
	}

	@Override
	public void setBoolean(int pos, boolean value) {
		currentTuple.setBoolean(pos,value);
	}

	@Override
	public void setObject(int pos, Object object) {
		currentTuple.setObject(pos,object);
	}

	@Override
	public <T> void setObject(Class<T> valueType, int pos, T value) {
		currentTuple.setObject(valueType,pos,value);
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
  public Object[] getArray() {
	  return currentTuple.getArray();
  }

	@Override
  public void setArray(Object[] array) {
		currentTuple.setArray(array);
	}
}

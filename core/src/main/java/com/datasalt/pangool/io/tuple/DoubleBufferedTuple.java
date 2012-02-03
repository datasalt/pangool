package com.datasalt.pangool.io.tuple;

import java.io.IOException;

import org.apache.hadoop.util.ReflectionUtils;

import com.datasalt.pangool.Schema;

public class DoubleBufferedTuple extends BaseTuple implements ITupleInternal{

  private ITuple currentTuple;
  private ITuple previousTuple;
  
  public DoubleBufferedTuple(ITuple tuple, ITuple oTuple) {
  	this.currentTuple = tuple;
  	this.previousTuple = oTuple;
  }
  
  public DoubleBufferedTuple(ITuple tuple){
  	this.currentTuple = tuple;
		this.previousTuple = ReflectionUtils.newInstance(Tuple.class,null);
  }
	
	public DoubleBufferedTuple(){
		this.currentTuple = ReflectionUtils.newInstance(Tuple.class,null);
		this.previousTuple = ReflectionUtils.newInstance(Tuple.class,null);
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
	public Object get(int pos) {
		return currentTuple.get(pos);
	}

	@Override
	public void set(int pos, Object object) {
		currentTuple.set(pos,object);
	}

	@Override
  public void clear() {
	  currentTuple.clear();
  }

	@Override
  public Schema getSchema() {
	  return currentTuple.getSchema();
  }


	@Override
  public Object get(String field) {
	  return currentTuple.get(field);
  }


	@Override
  public void set(String field, Object object) {
	  currentTuple.set(field,object);
  }

}

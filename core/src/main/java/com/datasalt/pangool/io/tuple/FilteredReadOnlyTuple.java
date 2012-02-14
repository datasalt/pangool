package com.datasalt.pangool.io.tuple;

import com.datasalt.pangool.Schema;
import com.datasalt.pangool.Schema.Field;

/**
 * A {@link ITuple} with a delegated one, but that creates a
 * read only view over it and filtering only for some fields. 
 * Useful for creating views over {@link ITuple} for some
 * particular fields.
 */
public class FilteredReadOnlyTuple implements ITuple {

	private ITuple delegated;
	private Schema schema;
	

	public FilteredReadOnlyTuple(Schema schema) { 
		this.schema = schema;
	}
	
	public void setDelegatedTuple(ITuple delegatedTuple) {
		delegated = delegatedTuple;
	}
	
	private void fail() {
		throw new RuntimeException("Trying to modify a read only tuple. This is not allowed");
	}

	@Override
  public Object get(int pos) {
		Field field = schema.getField(pos);
		return (field == null) ? null : delegated.get(field.getName());//TODO this needs to be accesssed directly with translation table
  }

	@Override
  public void set(int pos, Object object) {
		fail();
	}

	@Override
  public void clear() {
	  fail();
  }

	@Override
  public Schema getSchema() {
	  return schema;
  }
	
	@Override
  public Object get(String field) {
		Field f = schema.getField(field);
	  return (f == null) ? null : delegated.get(field);
  }

	@Override
  public void set(String field, Object object) {
	  fail();
  }
	
	public String toString(){
		return Tuple.toString(this);
	}
	
	
}

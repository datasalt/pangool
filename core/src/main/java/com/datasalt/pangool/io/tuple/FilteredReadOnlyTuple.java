package com.datasalt.pangool.io.tuple;

import com.datasalt.pangool.Schema;
import com.datasalt.pangool.Schema.Field;

/**
 * A {@link ITuple} with a delegated one, but that creates a
 * read only view over it and filtering only for some fields. 
 * Useful for creating views over {@link ITuple} for some
 * particular fields.
 */
public class FilteredReadOnlyTuple extends BaseTuple {

	private ITuple delegated;
	
	private Schema schema;
	
	public FilteredReadOnlyTuple(Schema schema) { //TODO this needs to accept translation table
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
	  String fieldName = schema.getField(pos).name(); //TODO this needs to be accesssed directly with translation table
	  return delegated.get(fieldName);
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
	  return delegated.getSchema(); //TODO this is wrong
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

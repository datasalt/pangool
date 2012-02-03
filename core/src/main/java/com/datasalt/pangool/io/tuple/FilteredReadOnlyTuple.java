package com.datasalt.pangool.io.tuple;

import java.util.Collection;
import java.util.HashSet;

import com.datasalt.pangool.Schema;

/**
 * A {@link ITuple} with a delegated one, but that creates a
 * read only view over it and filtering only for some fields. 
 * Useful for creating views over {@link ITuple} for some
 * particular fields.
 */
public class FilteredReadOnlyTuple extends BaseTuple {

	protected ITuple delegated;
	protected HashSet<String> filter;
	
	public FilteredReadOnlyTuple(Collection<String> filteredFields) {
		filter = new HashSet<String>(filteredFields);
	}
	
	public void setDelegatedTuple(ITuple delegatedTuple) {
		delegated = delegatedTuple;
	}
	
	private void fail() {
		throw new RuntimeException("Trying to modify a read only tuple. This is not allowed");
	}

	@Override
  public Object get(int pos) {
	  return delegated.get(pos);
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
	  return delegated.getSchema();
  }
	
	@Override
  public Object get(String field) {
	  return delegated.get(field);
  }

	@Override
  public void set(String field, Object object) {
	  fail();
  }
	
}

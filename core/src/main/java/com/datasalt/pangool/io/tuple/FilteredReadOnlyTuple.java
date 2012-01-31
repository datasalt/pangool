package com.datasalt.pangool.io.tuple;

import java.util.Collection;
import java.util.HashSet;

/**
 * A {@link ITuple} with a delegated one, but that creates a
 * read only view over it and filtering only for some fields. 
 * Useful for creating views over {@link ITuple} for some
 * particular fields.
 */
public class FilteredReadOnlyTuple implements ITuple {

	protected ITuple delegated;
	protected HashSet<String> filter;
	
	public FilteredReadOnlyTuple(Collection<String> filteredFields) {
		filter = new HashSet<String>(filteredFields);
	}
	
	public void setDelegatedTuple(ITuple delegatedTuple) {
		delegated = delegatedTuple;
	}
	
	private Object fail() {
		throw new RuntimeException("Trying to modify a read only tuple. This is not allowed");
	}

	@Override
  public int compareTo(ITuple o) {
	  return delegated.compareTo(o);
  }

	@Override
  public int partialHashCode(int fields) {
	  return delegated.partialHashCode(fields);
  }

	@Override
  public int size() {
	  return delegated.size();
  }

	@Override
  public Integer getInt(int pos) {
	  return (filter.contains(pos) ? delegated.getInt(pos) : null);
  }

	@Override
  public Long getLong(int pos) {
	  return (filter.contains(pos) ? delegated.getLong(pos) : null);
  }

	@Override
  public Float getFloat(int pos) {
	  return (filter.contains(pos) ? delegated.getFloat(pos) : null);
  }

	@Override
  public Double getDouble(int pos) {
	  return (filter.contains(pos) ? delegated.getDouble(pos) : null);
  }

	@Override
  public byte[] getString(int pos) {
	  return delegated.getString(pos);
  }

	@Override
  public Object getObject(int pos) {
	  return delegated.getObject(pos);
  }

	@Override
  public <T> T getObject(Class<T> clazz, int pos) {
	  return delegated.getObject(clazz, pos);
  }

	@Override
  public Enum<? extends Enum<?>> getEnum(int pos) {
	  return delegated.getEnum(pos);
  }

	@Override
  public void setEnum(int pos, Enum<? extends Enum<?>> value) {
		fail();
	}

	@Override
  public void setInt(int pos, int value) {
		fail();
	}

	@Override
  public void setString(int pos, byte[] value) {
		fail();
	}

	@Override
  public void setLong(int pos, long value) {
		fail();
	}

	@Override
  public void setFloat(int pos, float value) {
		fail();
	}

	@Override
  public void setDouble(int pos, double value) {
		fail();
	}

	@Override
  public void setBoolean(int pos, boolean value) {
		fail();
	}

	@Override
  public void setObject(int pos, Object object) {
		fail();
	}

	@Override
  public <T> void setObject(Class<T> valueType, int pos, T value) {
		fail();
	}

	
	@Override
	public String toString(){
		return delegated.toString();
	}

	@Override
  public void clear() {
		delegated.clear();
	}

	@Override
  public Object[] getArray() {
	  return delegated.getArray();
  }
	
	@Override
	public void setArray(Object[] array) {
		fail();
	}
}

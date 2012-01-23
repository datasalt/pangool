package com.datasalt.pangolin.grouper.io.tuple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datasalt.pangolin.grouper.Schema;
import com.google.common.collect.Sets;

/**
 * 
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
  public boolean isEmpty() {
		return delegated.isEmpty();
  }

	@Override
  public boolean containsKey(Object key) {
		return filter.contains(key) ? delegated.containsKey(key) : false;
  }

	@Override
  public boolean containsValue(Object value) {
		for (Entry<String, Object> entry : delegated.entrySet()) {
			
			if (entry.getValue().equals(value) && 
					!filter.contains(entry.getKey())) {
				return true;
			}
		}
		return false;
  }

	@Override
  public Object get(Object key) {
	  return (filter.contains(key)) ? delegated.get(key) : null;
  }

	@Override
  public Object put(String key, Object value) {
	  return fail();
  }

	@Override
  public Object remove(Object key) {
	  return fail();
  }

	@Override
  public void putAll(Map<? extends String, ? extends Object> m) {
		fail();
	}

	@Override
  public void clear() {
		fail();
  }

	@Override
  public Set<String> keySet() {
		Set<String> intersect =  new HashSet(delegated.keySet());
		intersect.retainAll(filter);
		return Collections.unmodifiableSet(intersect);
  }

	@Override
  public Collection<Object> values() {
		ArrayList<Object> vals = new ArrayList<Object>();
		for(String field: filter) {
			Object value = delegated.get(field);
			if (value != null) {
				vals.add(value);
			}
		}
	  return Collections.unmodifiableCollection(vals);
  }

	@Override
  public Set<java.util.Map.Entry<String, Object>> entrySet() {
		HashSet<java.util.Map.Entry<String, Object>> newSet = new HashSet<java.util.Map.Entry<String, Object>>();
		for(Entry<String, Object> entry: delegated.entrySet()) {
			if (filter.contains(entry.getKey())) {
				newSet.add(entry);
			}
		}
	  return Collections.unmodifiableSet(newSet);
  }

	@Override
  public int compareTo(ITuple o) {
	  return delegated.compareTo(o);
  }

	@Override
  public int partialHashCode(String[] fields) {
	  return delegated.partialHashCode(fields);
  }

	@Override
  public int size() {
	  return delegated.size();
  }

	@Override
  public Integer getInt(String fieldName) {
	  return (filter.contains(fieldName) ? delegated.getInt(fieldName) : null);
  }

	@Override
  public Long getLong(String fieldName) {
	  return (filter.contains(fieldName) ? delegated.getLong(fieldName) : null);
  }

	@Override
  public Float getFloat(String fieldName) {
	  return (filter.contains(fieldName) ? delegated.getFloat(fieldName) : null);
  }

	@Override
  public Double getDouble(String fieldName) {
	  return (filter.contains(fieldName) ? delegated.getDouble(fieldName) : null);
  }

	@Override
  public String getString(String fieldName) {
	  return delegated.getString(fieldName);
  }

	@Override
  public Object getObject(String fieldName) {
	  return delegated.getObject(fieldName);
  }

	@Override
  public <T> T getObject(Class<T> clazz, String fieldName) {
	  return delegated.getObject(clazz, fieldName);
  }

	@Override
  public Enum<? extends Enum<?>> getEnum(String fieldName) {
	  return delegated.getEnum(fieldName);
  }

	@Override
  public void setEnum(String fieldName, Enum<? extends Enum<?>> value) {
		fail();
	}

	@Override
  public void setInt(String fieldName, int value) {
		fail();
	}

	@Override
  public void setString(String fieldName, String value) {
		fail();
	}

	@Override
  public void setLong(String fieldName, long value) {
		fail();
	}

	@Override
  public void setFloat(String fieldName, float value) {
		fail();
	}

	@Override
  public void setDouble(String fieldName, double value) {
		fail();
	}

	@Override
  public void setBoolean(String fieldName, boolean value) {
		fail();
	}

	@Override
  public void setObject(String fieldName, Object object) {
		fail();
	}

	@Override
  public <T> void setObject(Class<T> valueType, String fieldName, T value) {
		fail();
	}

	
	@Override
	public String toString(){
		List<String> list = new ArrayList<String>(filter);
		return delegated.toString(list);
	}

	@Override
  public String toString(Collection<String> fields) {
		Set<String> set = new HashSet<String>(filter);
		set.retainAll(fields);
		List<String> fields2 = new ArrayList<String>(set);
	  return delegated.toString(fields2);
  }
}

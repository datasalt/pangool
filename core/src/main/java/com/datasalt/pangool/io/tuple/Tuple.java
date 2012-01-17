/**
 * Copyright [2011] [Datasalt Systems S.L.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasalt.pangool.io.tuple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is the basic implementation of {@link ITuple}. It extends
 * a HashMap<String, Object>
 */
public class Tuple extends HashMap<String,Object> implements ITuple {

  public Tuple() {
	}
		
	private void setField(String fieldName,Object value)  {
			put(fieldName,value);
	}
	
	@Override
	public Object put(String key, Object value) {
		if (key != null && value != null){
			return super.put(key,value);
		} else {
			return remove(key);
		}
	}
	
	@Override
	public Integer getInt(String fieldName)  {
		return (Integer)get(fieldName);
	}
	
	@Override
	public Long getLong(String fieldName)  {
		return (Long)get(fieldName);
	}
	
	
	@Override
	public Float getFloat(String fieldName)  {
		return (Float)get(fieldName);
	}
	
	@Override
	public Double getDouble(String fieldName)  {
		return (Double)get(fieldName);
	}
	
	@Override
	public String getString(String fieldName)  {
		return (String)get(fieldName);
	}
	
	@Override
	public Object getObject(String fieldName)  {
		return get(fieldName);
	}
	
	public Enum<?> getEnum(String fieldName)  {
		return (Enum<?>)get(fieldName);
	}
	
	@Override
	public void setEnum(String fieldName, Enum<? extends Enum<?>> value)  {
		setField(fieldName,value);
	}
	
	@Override
	public void setInt(String fieldName, int value)  {
		setField(fieldName,value);
	}
	
	@Override
	public void setString(String fieldName,String value)  {
		setField(fieldName,value);
	}
	
	@Override
	public void setLong(String fieldName,long value)  {
		setField(fieldName,value);
	}
	
	@Override
	public void setFloat(String fieldName,float value)  {
		setField(fieldName,value);
	}
	
	@Override
	public void setDouble(String fieldName,double value)  {
		setField(fieldName,value);
	}
	
	@Override
	public void setBoolean(String fieldName,boolean value)  {
		setField(fieldName,value);
	}
	
	@Override
	public void setObject(String fieldName,Object object)  {
		setField(fieldName,object);
	}
	
	@Override
  public <T> T getObject(Class<T> clazz, String fieldName)  {
	  return (T) get(fieldName);
  }

	@Override
  public <T> void setObject(Class<T> valueType, String fieldName, T value)  {
	  setField(fieldName,value);
  }	

	/**
	 * Calculates a combinated hashCode using the specified fields.
	 * @param fields
	 * @
	 */
	@Override
	public int partialHashCode(String[] fields)  {
		int result = 0;
		for(String fieldName : fields) {
			Object object = get(fieldName);
			int hashCode;
			if (object == null){
				hashCode = 0;		
			} else {
				hashCode = object.hashCode();
			}
			result = result * 31 + hashCode;
		}
		return result & Integer.MAX_VALUE;
	}

	@Override
  public int compareTo(ITuple that) {
		for (Map.Entry<String,Object> entry : entrySet()){
			String fieldName = entry.getKey();
			Object thisElement = get(fieldName);
			Object thatElement = get(fieldName);
			int comparison = SortComparator.compareObjects(thisElement,thatElement);
			if (comparison != 0){
				return comparison;
			}
		}
		return 0;
  }
	
	public static boolean leftEquals(ITuple tuple1,ITuple tuple2){
		if (!(tuple2 instanceof ITuple)){
			return false;
		}	
			
			for (Map.Entry<String,Object> entry : tuple1.entrySet()){
				String fieldName = entry.getKey();
				Object thisElement = entry.getValue();
				Object thatElement = ((ITuple) tuple2).getObject(fieldName);
				if (thisElement == null){
					if (thatElement != null){
						return false;
					}
				} else if(!thisElement.equals(thatElement)) {
					return false;
				}
			}
			return true;		
	}
	
	
	@Override
	public String toString() {
		return toString(keySet());		
	}
	
	/**
	 * If schema null then outputs the fields in order
	 * TODO this method needs to be reimplemented
	 */
	@Override
	public String toString(Collection<String> fields) {
		
			StringBuilder b = new StringBuilder("{"); // TODO not optimized,should be cached
			boolean first = true;
			
			List<String> orderedFields = new ArrayList<String>();
			orderedFields.addAll(keySet());
			
			for(String fieldName : fields) {

				Object element = get(fieldName);
				
				if(!first) {
					b.append(",");
				} else {
					first = false;
				}
				b.append("\"").append(fieldName).append("\"").append(":");
				if (element == null){
					b.append("null");
				} else {
						b.append(element.toString());
					
				}
			}
			b.append("}");
			return b.toString();
		}
}

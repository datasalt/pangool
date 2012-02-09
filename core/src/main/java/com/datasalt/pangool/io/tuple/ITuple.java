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

import org.apache.hadoop.io.Text;

import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.Schema.Field;

/**
 * This is the common interface implemented by {@link Tuple} and {@link DoubleBufferPangolinTuple}.
 * A Tuple is basically a map that can be used in Pangool for carrying data. 
 */
public interface ITuple /*,Comparable<ITuple>*/{

	public Schema getSchema();
	
	public void clear();
	
	public Object get(int pos);
	public void set(int pos, Object object);
	
	public void set(String field, Object object);
	public Object get(String field);
	
	public Integer getInt(int pos);
	public Integer getInt(String field);
	
	public Long getLong(int pos);
	public Long getLong(String field);
	
	public Float getFloat(int pos);
	public Float getFloat(String field);
	
	public Double getDouble(int pos);
	public Double getDouble(String field);
	
	public Text getString(int pos);
	public Text getString(String field);
	
	public Enum<? extends Enum<?>> getEnum(int pos);
	public Enum<? extends Enum<?>> getEnum(String field);
	

	
	// Setters
	
	public void setEnum(int pos, Enum<? extends Enum<?>> value);
	public void setEnum(String field, Enum<? extends Enum<?>> value);
	
	public void setInt(int pos, int value);
	public void setInt(String field, int value);
	
	public void setString(int pos, Text value);
	public void setString(String field, Text value);
	
	public void setString(int pos, String value);
	public void setString(String field, String value);
	
	public void setLong(int pos, long value) ;
	public void setLong(String field, long value) ;
	
	public void setFloat(int pos, float value) ;
	public void setFloat(String field, float value) ;
	
	public void setDouble(int pos, double value) ;
	public void setDouble(String field, double value) ;
	
	public void setBoolean(int pos, boolean value) ;
	public void setBoolean(String field, boolean value);
	
	
	
	public <T> void setObject(Class<T> valueType, int pos, T value) ;
	public <T> void setObject(Class<T> valueType,String field, T value) ;
	
	
	/**
	 * Thrown when a field is not present in schema
	 * 
	 *
	 */
	public static class InvalidFieldException extends CoGrouperException {
    private static final long serialVersionUID = 1L;

		public InvalidFieldException(String s,Throwable e) {
			super(s,e);
		}
		
		public InvalidFieldException(String s) {
			super(s);
		}
		
		public InvalidFieldException(Throwable e) {
			super(e);
		}
	}
	
	
	
}

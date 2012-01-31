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

import com.datasalt.pangool.CoGrouperException;

/**
 * This is the common interface implemented by {@link Tuple} and {@link DoubleBufferPangolinTuple}.
 * A Tuple is basically a map that can be used in Pangool for carrying data. 
 */
public interface ITuple extends Comparable<ITuple>{
	
	Object[] getArray();
	
	public void setArray(Object[] array);
	
	public void clear();
	
	public int partialHashCode(int nFields);
	
	public int size();
	
	public Integer getInt(int pos);
	
	public Long getLong(int pos);
	
	public Float getFloat(int pos);
	
	public Double getDouble(int pos);
	
	public byte[] getString(int pos);
	
	public Object getObject(int pos);
	
	public <T> T getObject(Class<T> clazz, int pos);
	
	public Enum<? extends Enum<?>> getEnum(int pos);
	
	// Setters
	
	public void setEnum(int pos, Enum<? extends Enum<?>> value);
	
	public void setInt(int pos, int value);
	
	public void setString(int pos, byte[] value);
	
	public void setLong(int pos, long value) ;
	
	public void setFloat(int pos, float value) ;
	
	public void setDouble(int pos, double value) ;
	
	public void setBoolean(int pos, boolean value) ;
	
	public void setObject(int pos, Object object) ;
	
	public <T> void setObject(Class<T> valueType, int pos, T value) ;
	
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

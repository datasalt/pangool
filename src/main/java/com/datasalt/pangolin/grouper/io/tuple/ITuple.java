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
package com.datasalt.pangolin.grouper.io.tuple;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.thrift.TBase;

import com.datasalt.pangolin.grouper.Schema;
import com.datasalt.pangolin.grouper.Grouper;
import com.datasalt.pangolin.grouper.GrouperException;

/**
 * This is the common interface implemented by {@link BaseTuple} and {@link Tuple}.
 * A Tuple is basically a hadoop-serializable object containing fields according to the schema defined in 
 * {@link Schema}. Tuples are used in intermediate {@link org.apache.hadoop.mapreduce.Mapper}
 * outputs in {@link Grouper} and {@link Grouper}
 * 
 * @author eric
 *
 */
public interface ITuple extends WritableComparable<ITuple>,Configurable{
	
	public Schema getSchema();
	
	public int partialHashCode(String[] fields) throws InvalidFieldException;
	
	public void setSchema(Schema schema);
	
	
	//Getters
	
	public int getInt(String fieldName) throws InvalidFieldException;
	
	public long getLong(String fieldName) throws InvalidFieldException;
	
	public float getFloat(String fieldName) throws InvalidFieldException;
	
	public double getDouble(String fieldName) throws InvalidFieldException;
	
	public String getString(String fieldName) throws InvalidFieldException;
	
	public Object getObject(String fieldName) throws InvalidFieldException;
	
	public <T> T getObject(Class<T> clazz,String fieldName) throws InvalidFieldException ;
	
	public Enum<? extends Enum<?>> getEnum(String fieldName) throws InvalidFieldException;
	
	
	
	//Setters
	
	public void setEnum(String fieldName, Enum<? extends Enum<?>> value) throws InvalidFieldException;
	
	public void setInt(String fieldName, int value) throws InvalidFieldException;
	
	public void setString(String fieldName,String value) throws InvalidFieldException;
	
	public void setLong(String fieldName,long value) throws InvalidFieldException;
	
	public void setFloat(String fieldName,float value) throws InvalidFieldException;
	
	public void setDouble(String fieldName,double value) throws InvalidFieldException;
	
	public void setBoolean(String fieldName,boolean value) throws InvalidFieldException;
	
	public void setObject(String fieldName,Object object) throws InvalidFieldException;
	
	public <T> void setObject(Class<T> valueType,String fieldName,T value) throws InvalidFieldException;
	
	public void setThriftObject(String fieldName,TBase<?, ?> value) throws InvalidFieldException;
	

	public String toString(int minFieldIndex,int maxFieldIndex) throws InvalidFieldException;
	
	/**
	 * Thrown when a field is not present in schema
	 * 
	 *
	 */
	public static class InvalidFieldException extends GrouperException {
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

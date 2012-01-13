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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;

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
public interface ITuple extends Map<String,Object>,Comparable<ITuple>/*extends WritableComparable<ITuple>,Configurable*/{
	
	//public Schema getSchema();
	
	public int partialHashCode(String[] fields);
	
	//public void setSchema(Schema schema);
	
	
	//Getters
	
	
	public int size();
	
	public Integer getInt(String fieldName);
	
	public Long getLong(String fieldName);
	
	public Float getFloat(String fieldName);
	
	public Double getDouble(String fieldName);
	
	public String getString(String fieldName);
	
	public Object getObject(String fieldName);
	
	public <T> T getObject(Class<T> clazz,String fieldName);
	
	public Enum<? extends Enum<?>> getEnum(String fieldName);
	
	
	
	//Setters
	
	public void setEnum(String fieldName, Enum<? extends Enum<?>> value);
	
	public void setInt(String fieldName, int value);
	
	public void setString(String fieldName,String value);
	
	public void setLong(String fieldName,long value) ;
	
	public void setFloat(String fieldName,float value) ;
	
	public void setDouble(String fieldName,double value) ;
	
	public void setBoolean(String fieldName,boolean value) ;
	
	public void setObject(String fieldName,Object object) ;
	
	public <T> void setObject(Class<T> valueType,String fieldName,T value) ;
	
	public String toString(Collection<String> fields);
		
	
	
//	public void write(Schema schema,DataOutput output) throws IOException;
//	public void readFields(Schema schema,DataInput input) throws IOException;
//	
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

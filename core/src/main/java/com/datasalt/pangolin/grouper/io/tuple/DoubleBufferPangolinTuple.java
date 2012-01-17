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
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.thrift.TBase;

import com.datasalt.pangolin.grouper.Schema;
import com.datasalt.pangolin.grouper.mapreduce.RollupReducer;

/**
 * 
 * This implementation of {@link ITuple} allows to mantain two {@link Tuple} instances in a double-buffered fashion.
 * That is, when the {@link #readFields(DataInput)} method is called , the last previous state is kept and then the new current 
 * instance is updated.
 * 
 *  
 * Since this double buffered mechanism avoids cloning or deep copying instances ,allows efficient comparison between
 * tuples performed in {@link RollupReducer}.
 * 
 * @author eric
 * 
 */
public class DoubleBufferPangolinTuple implements ITuple {

	//private Schema schema;
	//private Configuration conf;
	private Tuple previousTuple, currentTuple;

//	@SuppressWarnings("unused")
	public DoubleBufferPangolinTuple() {
		// this allows to use private construction
		previousTuple = ReflectionUtils.newInstance(Tuple.class, null);
		currentTuple = ReflectionUtils.newInstance(Tuple.class, null);
	}

//	Tuple(@Nonnull Schema schema) {
//		//this.schema = schema;
//		currentTuple = new BaseTuple(schema);
//		previousTuple = new BaseTuple(schema);
//	}
//	
//	Tuple(@Nonnull Configuration conf){
//		currentTuple = new BaseTuple(conf);
//		previousTuple = new BaseTuple(conf);
//	}
	
	

//	@Override
//	public void write(Schema schema,DataOutput out) throws IOException {
//		currentTuple.write(schema,out);
//
//	}
//
//	/**
//	 * This is where the double-buffer swapping is performed.
//	 */
//	@Override
//	public void readFields(Schema schema,DataInput in) throws IOException {
//		// swapping double buffering
//		BaseTuple tmpTuple = previousTuple;
//		previousTuple = currentTuple;
//		currentTuple = tmpTuple;
//
//		currentTuple.readFields(schema,in);
//	}
	
	public void swapInstances() throws IOException {
		Tuple tmpTuple = previousTuple;
		previousTuple = currentTuple;
		currentTuple = tmpTuple;
	}

	/**
	 * It always return a nonnull instance even if readFields never was invoked
	 *
	 */
	public @Nonnull Tuple getPreviousTuple() {
		return previousTuple;
	}

	@Override
	public int compareTo(ITuple anotherTuple) {
		return currentTuple.compareTo(anotherTuple);
	}

//	@Override
//	public void setConf(Configuration conf) {
//		if(conf != null) {
//			//this.conf = conf;
//			//try {
////				Schema schema = Schema.parse(this.conf);
////				if(schema != null) {
////					this.schema = schema;
////				}
//				previousTuple.setConf(conf);
//				currentTuple.setConf(conf);
//			//} catch(GrouperException e) {
//			//	throw new RuntimeException(e);
//			//}
//		}
//
//	}
//
//	@Override
//	public Configuration getConf() {
//		return currentTuple.getConf();
//	}

//	@Override
//	public Schema getSchema() {
//		return currentTuple.getSchema();
//	}

	@Override
	public int partialHashCode(String[] fields)  {
		return currentTuple.partialHashCode(fields);
	}

	@Override
	public Integer getInt(String fieldName)  {
		return currentTuple.getInt(fieldName);
	}

	@Override
	public Long getLong(String fieldName)  {
		return currentTuple.getLong(fieldName);
	}

	@Override
	public Float getFloat(String fieldName)  {
		return currentTuple.getFloat(fieldName);
	}

	@Override
	public Double getDouble(String fieldName)  {
		return currentTuple.getDouble(fieldName);
	}

	@Override
	public String getString(String fieldName)  {
		return currentTuple.getString(fieldName);
	}

	@Override
	public Object getObject(String fieldName)  {
		return currentTuple.getObject(fieldName);
	}
	
	@Override
  public <T> T getObject(Class<T> clazz, String fieldName)  {
	  return currentTuple.getObject(clazz,fieldName);
  }

	@Override
	public Enum<? extends Enum<?>> getEnum(String fieldName)  {
		return currentTuple.getEnum(fieldName);
	}

	
	//set enums
	
	
	@Override
	public void setEnum(String fieldName, Enum<? extends Enum<?>> value)  {
		currentTuple.setEnum(fieldName, value);

	}

	@Override
	public void setInt(String fieldName, int value)  {
		currentTuple.setInt(fieldName, value);

	}

	@Override
	public void setString(String fieldName, String value)  {
		currentTuple.setString(fieldName, value);

	}

	@Override
	public void setLong(String fieldName, long value)  {
		currentTuple.setLong(fieldName, value);

	}

	@Override
	public void setFloat(String fieldName, float value)  {
		currentTuple.setFloat(fieldName, value);

	}

	@Override
	public void setDouble(String fieldName, double value)  {
		currentTuple.setDouble(fieldName, value);

	}

	@Override
	public void setBoolean(String fieldName, boolean value)  {
		currentTuple.setBoolean(fieldName, value);

	}

	@Override
	public void setObject(String fieldName, Object object)  {
		currentTuple.setObject(fieldName, object);

	}

//	@Override
//	public void setThriftObject(String fieldName,TBase<?,?> value)  {
//		currentTuple.setThriftObject(fieldName, value);
//	}
	
	@Override
  public <T> void setObject(Class<T> valueType, String fieldName, T value)  {
	  currentTuple.setObject(valueType,fieldName,value);
  }
	

	@Override
	public String toString() {
		return currentTuple.toString();
	}

	@Override
	public boolean equals(Object tuple){
		return currentTuple.equals(tuple);
	}

	@Override
	public void clear() {
		currentTuple.clear();
		
	}

	@Override
	public boolean containsKey(Object key) {
		return currentTuple.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return currentTuple.containsValue(value);
	}

	@Override
	public Set<java.util.Map.Entry<String, Object>> entrySet() {
		return currentTuple.entrySet();
	}

	@Override
	public Object get(Object key) {
		return currentTuple.get(key);
	}

	@Override
	public boolean isEmpty() {
		return currentTuple.isEmpty();
	}

	@Override
	public Set<String> keySet() {
		return currentTuple.keySet();
	}

	@Override
	public Object put(String key, Object value) {
		return currentTuple.put(key,value);
	}

	@Override
	public void putAll(Map<? extends String, ? extends Object> m) {
		currentTuple.putAll(m);
		
	}

	@Override
	public Object remove(Object key) {
		return currentTuple.remove(key);
	}

	@Override
	public Collection<Object> values() {
		return currentTuple.values();
	}

	@Override
	public int size() {
		return currentTuple.size();
	}

	@Override
  public String toString(Collection<String> fields) {
	  return currentTuple.toString(fields);
  }
}

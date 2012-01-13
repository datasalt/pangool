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
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.thrift.TBase;

import com.datasalt.pangolin.commons.Buffer;
import com.datasalt.pangolin.grouper.Schema;
import com.datasalt.pangolin.grouper.Schema.Field;
import com.datasalt.pangolin.grouper.GrouperException;
import com.datasalt.pangolin.io.Serialization;

/**
 * This is the basic implementation of {@link ITuple}. 
 * 
 * @author eric
 * 
 */
public class BaseTuple extends HashMap<String,Object> implements ITuple {
	//private Configuration conf;
	
	//private Map<String,Object> elements = new HashMap<String,Object>();
	//private Set<String> nullObjects = new HashSet<String>();
	
	
	//private Serialization serialization;
	//private Schema schema;
//	private Text text = new Text();
	
	
	
	
	
//	/**
//	 * Hadoop can use this using ReflectionUtils.newInstance
//	 */
//	@SuppressWarnings("unused")
  public BaseTuple() {
	}
	
//	BaseTuple(@Nonnull Schema schema){
//		setSchema(schema);
//	}
	
//	BaseTuple(@Nonnull Configuration conf){
//		try {
//	    Schema schema = Schema.parse(conf);
//	    if (schema == null){
//	    	throw new RuntimeException("schema must be set in conf");
//	    }
//    } catch(GrouperException e) {
//	    throw new RuntimeException(e);
//    }
//		setConf(conf);
//	}
	

//	@Override
//	public Schema getSchema(){
//		return schema;
//	}
	
//	public void setSerialization(Serialization ser){
//		this.serialization = ser;
//	}
	
//	public void clear(){
//		this.
//		this.elements.clear();
////		this.nullObjects.clear();
////		populateObjects();
//	}
	
	
	
	
//	@Override
//	public void setSchema(@Nonnull Schema schema) {
//		if (this.schema != null){
//			throw new IllegalStateException("Schema already set,not allowed to set it twice");
//		} else if (schema == null){
//			throw new RuntimeException("Schema can't be null");
//		}
//		this.schema = schema;
//		this.cachedEnums.clear();
//		cacheEnums(schema);
//		clear();
//	}
	
//	private void populateObjects(){
//		for (Field field : schema.getFields()){
//			Class fieldType = field.getType();
//			String fieldName = field.getName();
//			if (fieldType == Integer.class || fieldType == VIntWritable.class){
//				tupleElements.put(fieldName,0);
//			} else if (fieldType == Long.class || fieldType == VLongWritable.class){
//				tupleElements.put(fieldName,0l);
//			} else if(fieldType == String.class){
//				tupleElements.put(fieldName,"");
//			} else if(fieldType == Boolean.class){
//				tupleElements.put(fieldName,false);
//			} else if(fieldType == Float.class){
//				tupleElements.put(fieldName,0.f);
//			} else if(fieldType == Double.class){
//				tupleElements.put(fieldName,0.0);
//			} else if(fieldType.isEnum()){
//        Enum[] enums = cachedEnums.get(fieldName);
//        tupleElements.put(fieldName,enums[0]);
//			} else {
//				Object object = ReflectionUtils.newInstance(fieldType, conf);
//				tupleElements.put(fieldName,object);
//				nullObjects.add(fieldName);
//			}
//		}
//	}
	
	
	
		
	
	private Object getField(String fieldName)  {
//		if (!this.schema.containsFieldName(fieldName)){
//			throw new InvalidFieldException("Field " + fieldName + " not in schema");
//		}
		
//		if (nullObjects.contains(fieldName)){
//			return null;
//		} else {
			Object ret = get(fieldName);
//			if (ret == null){
//				throw new InvalidFieldException("Field '"+fieldName + "' not in present in Tuple");
//			}
			return ret;
		
	}
	
	private void setField(String fieldName,Object value)  {
//		if (!this.schema.containsFieldName(fieldName)){
//			throw new InvalidFieldException("Field \"" + fieldName + "\" not in schema");
//		}
		
		//checkValidValueForField(fieldName,value);
		
//		if (value == null){
//			nullObjects.add(fieldName);
//		} else {
//			nullObjects.remove(fieldName);
			put(fieldName,value);
//		}
	}
	
//	private void checkType(String fieldName,Object value,Class ... expectedClasses)  {
//		for (Class expectedClass : expectedClasses){
//			if (value.getClass() == expectedClass){
//				return;
//			}
//		}
//		
//		String concatedClasses=expectedClasses[0].toString();
//		for (int i=1 ; i < expectedClasses.length ; i++){
//			concatedClasses += "," + expectedClasses[i].toString();
//		}
//		
//		throw new InvalidFieldException("Value " + value + " for field " + fieldName + " doesn't match expected types :" + concatedClasses);
//	}
	
//	private void checkValidValueForField(String fieldName,Object value)   {
//		Class<?> expectedType = this.schema.getField(fieldName).getType();
//		
//		if (expectedType == Integer.class || expectedType == VIntWritable.class){
//			checkNonNull(fieldName, value);
//			checkType(fieldName,value,Integer.class);
//		} else if (expectedType == Long.class || expectedType == VLongWritable.class){
//			//checkNonNull(fieldName,value);
//			//checkType(fieldName,value,)
//		}
//		
//		if (value instanceof Integer){
//			
//		} else  if (value instanceof Long){
//			checkNonNull(fieldName,value);
//		} else if (value instanceof String){
//			checkNonNull(fieldName,value);
//		} else if (value instanceof Float){
//			checkNonNull(fieldName,value);
//		} else if (value instanceof Double){
//			checkNonNull(fieldName,value);
//		} else if (value instanceof Boolean){
//			checkNonNull(fieldName,value);
//		} else if (value.getClass().isEnum()){
//			checkNonNull(fieldName,value);
//		} else {
//			
//		}
//		
//	}
	
//	private void checkNonNull(String fieldName,Object value)  {
//		if (value == null){
//			throw new InvalidFieldException("Field " + fieldName + " can't be null");
//		}
//	}
	
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
		return (Integer)getField(fieldName);
	}
	
	@Override
	public Long getLong(String fieldName)  {
		return (Long)getField(fieldName);
	}
	
	
	@Override
	public Float getFloat(String fieldName)  {
		return (Float)getField(fieldName);
	}
	
	@Override
	public Double getDouble(String fieldName)  {
		return (Double)getField(fieldName);
	}
	
	@Override
	public String getString(String fieldName)  {
		return (String)getField(fieldName);
	}
	
	@Override
	public Object getObject(String fieldName)  {
		return getField(fieldName);
	}
	
	public Enum<?> getEnum(String fieldName)  {
		return (Enum<?>)getField(fieldName);
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
	  return (T) getField(fieldName);
  }

	@Override
  public <T> void setObject(Class<T> valueType, String fieldName, T value)  {
	  setField(fieldName,value);
  }
	
//	private void throwIOIfNull(Field field) throws IOException{
//		if (nullObjects.contains(field.getName())){
//			throw new IOException("Field '" + field.getName() + "' with type '" + field.getType().getName() + "' can't be serialized as null");
//		}
//	}
	
	
//	}

	
	
//	public void deepCopyFrom(TupleImpl tuple) {
//		setSchema(tuple.getSchema());
//		try{
//			for(Field field : schema.getFields()) {
//				String fieldName = field.getName();
//				// TODO make deep copy !!!!
//				setField(fieldName, tuple.getField(fieldName));
//
//			}
//		} catch(InvalidFieldException e) {
//			// shouldn't occur
//			throw new RuntimeException(e);
//		}
//	}

	/**
	 * Calculates a combinated hashCode using the specified fields.
	 * @param fields
	 * @
	 */
	@Override
	public int partialHashCode(String[] fields)  {
		int result = 0;
		for(String fieldName : fields) {
			Object object = getField(fieldName);
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

//	@Override
//  public Configuration getConf() {
//		return this.conf;
//  }
//
//	/**
//	 * This method is used automatically in Hadoop in reducer step, when it instanciates the keys/values for first time.
//	 */
//	@Override
//  public void setConf(Configuration conf) {
//		if (this.conf != null){
//			//TODO should be so strict ?
//			throw new IllegalStateException("Tuple previously configured.Not allowed to be configured twice");
//		}
//		if (conf != null){
//			this.conf = conf;
//			try {
//				Schema schema =Schema.parse(this.conf);
//				if (schema != null){
//					setSchema(schema);
//				}
//	      this.serialization = new Serialization(conf);
//      } catch(GrouperException e) {
//	      throw new RuntimeException(e);
//      } catch(IOException e) {
//	      throw new RuntimeException(e);
//      }
//		}
//  }

	@Override
  public int compareTo(ITuple that) {
		//TODO this method makes sense ? Mark it like NotImplemented ?
//		try{
//		if (!this.schema.equals(that.getSchema())){
//			//TODO is this necessary ? Too heavy
//			//TODO should Int and VInt treated different ? 
//			throw new RuntimeException("Schemas are different + "+schema + " <=>" +that.getSchema()); 
//		}
		for (Map.Entry<String,Object> entry : entrySet()){
		//for (Field field : this.schema.getFields()){
			String fieldName = entry.getKey();
			//String fieldName = field.getName();
			Object thisElement = getField(fieldName);
			Object thatElement = getField(fieldName);
			int comparison = SortComparator.compareObjects(thisElement,thatElement);
			if (comparison != 0){
				return comparison;
			}
		}
		return 0;
//		} catch(InvalidFieldException e){
//			throw new RuntimeException(e);
//		}
  }
	
	
	
	
	//@Override
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
	 * @param schema
	 * @param minFieldIndex
	 * @param maxFieldIndex
	 * @return
	 * @
	 */
	@Override
	public String toString(Collection<String> fields) {
		
			StringBuilder b = new StringBuilder("{"); // TODO not optimized,should be cached
			boolean first = true;
			
			List<String> orderedFields = new ArrayList<String>();
			orderedFields.addAll(keySet());
			
			for(String fieldName : fields) {

				Object element = getField(fieldName);
				
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

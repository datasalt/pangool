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
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
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
import com.datasalt.pangolin.grouper.FieldsDescription;
import com.datasalt.pangolin.grouper.FieldsDescription.Field;
import com.datasalt.pangolin.grouper.GrouperException;
import com.datasalt.pangolin.io.Serialization;

/**
 * This is the basic implementation of {@link ITuple}. It implements the type-checking and raw fields 
 * serialization/deserialization.
 * 
 * @author eric
 * 
 */
public class BaseTuple implements ITuple {
	private Configuration conf;
	
	private Map<String,Object> tupleElements = new HashMap<String,Object>();
	private Set<String> nullObjects = new HashSet<String>();
	private DataOutputBuffer tmpOutputBuffer = new DataOutputBuffer();
	private Buffer tmpInputBuffer = new Buffer();
	private Serialization serialization;
	private FieldsDescription schema;
	private Text text = new Text();
	
	@SuppressWarnings("rawtypes")
  private Map<String,Enum[]> cachedEnums = new HashMap<String,Enum[]>();
	
	
	
	/**
	 * Hadoop can use this using ReflectionUtils.newInstance
	 */
	@SuppressWarnings("unused")
  private BaseTuple() {
	}
	
	BaseTuple(@Nonnull FieldsDescription schema){
		setSchema(schema);
	}

	@Override
	public FieldsDescription getSchema(){
		return schema;
	}
	
	public void setSerialization(Serialization ser){
		this.serialization = ser;
	}
	
	public void clear(){
		this.tupleElements.clear();
		this.nullObjects.clear();
		populateObjects();
	}
	
	
	/**
	 * Caches the values from the enum fields. This is done for efficiency since uses reflection. 
	 * 
	 */
	private void cacheEnums(FieldsDescription schema) {
		try {
			for(Field field : schema.getFields()) {
				Class<?> type = field.getType();
				if(type.isEnum()) {
					Method method = type.getMethod("values", null);
					Object values = method.invoke(null);
					cachedEnums.put(field.getName(),(Enum[])values);
				}

			}
		} catch(Exception e) {
			throw new RuntimeException(e);
		}

	}
	
	@Override
	public void setSchema(@Nonnull FieldsDescription schema) {
		this.schema = schema;
		this.cachedEnums.clear();
		cacheEnums(schema);
		clear();
	}
	
	private void populateObjects(){
		for (Field field : schema.getFields()){
			Class fieldType = field.getType();
			String fieldName = field.getName();
			if (fieldType == Integer.class || fieldType == VIntWritable.class){
				tupleElements.put(fieldName,0);
			} else if (fieldType == Long.class || fieldType == VLongWritable.class){
				tupleElements.put(fieldName,0l);
			} else if(fieldType == String.class){
				tupleElements.put(fieldName,"");
			} else if(fieldType == Boolean.class){
				tupleElements.put(fieldName,false);
			} else if(fieldType == Float.class){
				tupleElements.put(fieldName,0.f);
			} else if(fieldType == Double.class){
				tupleElements.put(fieldName,0.0);
			} else if(fieldType.isEnum()){
        Enum[] enums = cachedEnums.get(fieldName);
        tupleElements.put(fieldName,enums[0]);
			} else {
				Object object = ReflectionUtils.newInstance(fieldType, conf);
				tupleElements.put(fieldName,object);
				nullObjects.add(fieldName);
			}
		}
	}
	
	
	
		
	
	private Object getField(String fieldName) throws InvalidFieldException {
		if (!this.schema.containsFieldName(fieldName)){
			throw new InvalidFieldException("Field " + fieldName + " not in schema");
		}
		
		if (nullObjects.contains(fieldName)){
			return null;
		} else {
			return tupleElements.get(fieldName);
		}
	}
	
	private void setField(String fieldName,Object value) throws InvalidFieldException {
		if (!this.schema.containsFieldName(fieldName)){
			throw new InvalidFieldException("Field \"" + fieldName + "\" not in schema");
		}
		
		checkValidValueForField(fieldName,value);
		
		if (value == null){
			nullObjects.add(fieldName);
		} else {
			nullObjects.remove(fieldName);
			tupleElements.put(fieldName,value);
		}
	}
	
	private void checkType(String fieldName,Object value,Class ... expectedClasses) throws InvalidFieldException {
		for (Class expectedClass : expectedClasses){
			if (value.getClass() == expectedClass){
				return;
			}
		}
		
		String concatedClasses=expectedClasses[0].toString();
		for (int i=1 ; i < expectedClasses.length ; i++){
			concatedClasses += "," + expectedClasses[i].toString();
		}
		
		throw new InvalidFieldException("Value " + value + " for field " + fieldName + " doesn't match expected types :" + concatedClasses);
	}
	
	private void checkValidValueForField(String fieldName,Object value)  throws InvalidFieldException {
		Class<?> expectedType = this.schema.getField(fieldName).getType();
		
		if (expectedType == Integer.class || expectedType == VIntWritable.class){
			checkNonNull(fieldName, value);
			checkType(fieldName,value,Integer.class);
		} else if (expectedType == Long.class || expectedType == VLongWritable.class){
			//checkNonNull(fieldName,value);
			//checkType(fieldName,value,)
		}
		
		if (value instanceof Integer){
			
		} else  if (value instanceof Long){
			checkNonNull(fieldName,value);
		} else if (value instanceof String){
			checkNonNull(fieldName,value);
		} else if (value instanceof Float){
			checkNonNull(fieldName,value);
		} else if (value instanceof Double){
			checkNonNull(fieldName,value);
		} else if (value instanceof Boolean){
			checkNonNull(fieldName,value);
		} else if (value.getClass().isEnum()){
			checkNonNull(fieldName,value);
		} else {
			
		}
		
	}
	
	private void checkNonNull(String fieldName,Object value) throws InvalidFieldException {
		if (value == null){
			throw new InvalidFieldException("Field " + fieldName + " can't be null");
		}
	}
	
	@Override
	public int getInt(String fieldName) throws InvalidFieldException {
		return (Integer)getField(fieldName);
	}
	
	@Override
	public long getLong(String fieldName) throws InvalidFieldException {
		return (Long)getField(fieldName);
	}
	
	
	@Override
	public float getFloat(String fieldName) throws InvalidFieldException {
		return (Float)getField(fieldName);
	}
	
	@Override
	public double getDouble(String fieldName) throws InvalidFieldException {
		return (Double)getField(fieldName);
	}
	
	@Override
	public String getString(String fieldName) throws InvalidFieldException {
		return (String)getField(fieldName);
	}
	
	@Override
	public Object getObject(String fieldName) throws InvalidFieldException {
		return getField(fieldName);
	}
	
	public Enum<?> getEnum(String fieldName) throws InvalidFieldException {
		return (Enum<?>)getField(fieldName);
	}
	
	@Override
	public void setEnum(String fieldName, Enum<? extends Enum<?>> value) throws InvalidFieldException {
		setField(fieldName,value);
	}
	
	@Override
	public void setInt(String fieldName, int value) throws InvalidFieldException {
		setField(fieldName,value);
	}
	
	@Override
	public void setString(String fieldName,String value) throws InvalidFieldException {
		setField(fieldName,value);
	}
	
	@Override
	public void setLong(String fieldName,long value) throws InvalidFieldException {
		setField(fieldName,value);
	}
	
	@Override
	public void setFloat(String fieldName,float value) throws InvalidFieldException {
		setField(fieldName,value);
	}
	
	@Override
	public void setDouble(String fieldName,double value) throws InvalidFieldException {
		setField(fieldName,value);
	}
	
	@Override
	public void setBoolean(String fieldName,boolean value) throws InvalidFieldException {
		setField(fieldName,value);
	}
	
	@Override
	public void setObject(String fieldName,Object object) throws InvalidFieldException {
		setField(fieldName,object);
	}
	
	@Override
	public void setThriftObject(String fieldName,TBase value) throws InvalidFieldException{
		setField(fieldName,value);
	}
	
	@Override
  public <T> T getObject(Class<T> clazz, String fieldName) throws InvalidFieldException {
	  return (T) getField(fieldName);
  }

	@Override
  public <T> void setObject(Class<T> valueType, String fieldName, T value) throws InvalidFieldException {
	  setField(fieldName,value);
  }
	

	@Override
	public void write(DataOutput output) throws IOException {
		for (Field field : schema.getFields()) {
			String fieldName = field.getName();
			Class<?> fieldType = field.getType();
			Object element = tupleElements.get(fieldName);
			try{
			if (fieldType == VIntWritable.class) {
				WritableUtils.writeVInt(output, (Integer) element);
			} else if (fieldType == VLongWritable.class) {
				WritableUtils.writeVLong(output, (Long) element);
			} else if (fieldType == Integer.class){ 
			  output.writeInt((Integer)element);
			} else if (fieldType == Long.class){
				output.writeLong((Long)element);
			} else if (fieldType == Double.class) {
				output.writeDouble((Double) element);
			} else if (fieldType == Float.class) {
				output.writeFloat((Float) element);
			} else if (fieldType == String.class) {
				text.set((String)element);
				text.write(output);
			}	else if (fieldType == Boolean.class) {
				output.writeBoolean((Boolean)element);
			} else if (fieldType.isEnum()){
				Enum<?> e = (Enum<?>)element;
				WritableUtils.writeVInt(output,e.ordinal());
			} else {
				Object object = tupleElements.get(fieldName);
				if (object == null){
					WritableUtils.writeVInt(output,0);
				} else {
					tmpOutputBuffer.reset();
					serialization.ser(object,tmpOutputBuffer);
					WritableUtils.writeVInt(output,tmpOutputBuffer.getLength());
					output.write(tmpOutputBuffer.getData(),0,tmpOutputBuffer.getLength());
				}
			}} catch(ClassCastException e){
				throw new IOException("Field '" + fieldName  + "' contains '" + element + 
						"' which is " + element.getClass().getName() + ".The expected type is " + fieldType.getName());
			}
		}
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		for (int i =0 ; i < schema.getFields().length ; i++) {
			Class<?> fieldType = schema.getFields()[i].getType();
			String name = schema.getFields()[i].getName();
			if (fieldType == VIntWritable.class) {
				tupleElements.put(name,WritableUtils.readVInt(input));
			} else if (fieldType == VLongWritable.class) {
				tupleElements.put(name,WritableUtils.readVLong(input));
			} else if (fieldType == Integer.class){
				tupleElements.put(name,input.readInt());
			} else if (fieldType == Long.class){
				tupleElements.put(name, input.readLong());
			}	else if (fieldType == Double.class) {
				tupleElements.put(name, input.readDouble());
			} else if (fieldType == Float.class) {
				tupleElements.put(name, input.readFloat());
			} else if (fieldType == String.class) {
				text.readFields(input);
				tupleElements.put(name, text.toString());
			} else if (fieldType == Boolean.class) {
				tupleElements.put(name, input.readBoolean());
			} else if (fieldType.isEnum()){
				int ordinal = WritableUtils.readVInt(input);
				try{
					Enum<?>[] enums = cachedEnums.get(name);
					if (enums == null){
						throw new IOException("Field "+ name + " is not a enum type");
					}
					tupleElements.put(name,enums[ordinal]);
				} catch (ArrayIndexOutOfBoundsException e){
					throw new RuntimeException(e);
				}
			} else {
				int size =WritableUtils.readVInt(input);
				if (size != 0){
					tmpInputBuffer.setSize(size);
					input.readFully(tmpInputBuffer.getBytes(),0,size);
					Object ob = serialization.deser(tupleElements.get(name),tmpInputBuffer.getBytes(),0,size);
					this.tupleElements.put(name, ob);
					nullObjects.remove(name);
				} else {
					nullObjects.add(name);
				}
			}
		}
	}
	
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
	 * @throws InvalidFieldException
	 */
	@Override
	public int partialHashCode(String[] fields) throws InvalidFieldException {
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

	@Override
  public Configuration getConf() {
		return this.conf;
  }

	/**
	 * This method is used automatically in Hadoop in reducer step, when it instanciates the keys/values for first time.
	 */
	@Override
  public void setConf(Configuration conf) {
		if (conf != null){
			this.conf = conf;
			try {
				FieldsDescription schema =FieldsDescription.parse(this.conf);
				if (schema != null){
					setSchema(schema);
				}
	      this.serialization = new Serialization(conf);
      } catch(GrouperException e) {
	      throw new RuntimeException(e);
      } catch(IOException e) {
	      throw new RuntimeException(e);
      }
		}
  }

	@Override
  public int compareTo(ITuple that) {
		//TODO this method makes sense ? Mark it like NotImplemented ?
		try{
		if (!this.schema.equals(that.getSchema())){
			//TODO is this necessary ? Too heavy
			//TODO should Int and VInt treated different ? 
			throw new RuntimeException("Schemas are different"); 
		}
		
		for (Field field : this.schema.getFields()){
			String fieldName = field.getName();
			Object thisElement = getField(fieldName);
			Object thatElement = getField(fieldName);
			int comparison = compareObjects(thisElement,thatElement);
			if (comparison != 0){
				return comparison;
			}
		}
		return 0;
		} catch(InvalidFieldException e){
			throw new RuntimeException(e);
		}
  }
	
	private static int compareObjects(Object element1, Object element2 ){

		if (element1 == null){
			return (element2 == null) ? 0 : -1;
		} else  {
			if (element1 instanceof Comparable){
				return ((Comparable) element1).compareTo(element2);
			} else {
				//TODO what to do here ? 
				return 0; //TODO;
			}
		}
	}
	
	
	@Override
	public boolean equals(Object tuple2){
		if (!(tuple2 instanceof BaseTuple)){
			return false;
		}
		
		try {
			for(Field field : schema.getFields()) {
				String fieldName = field.getName();
				Object thisElement = getField(fieldName);
				Object thatElement = ((BaseTuple) tuple2).getField(fieldName);
				if (thisElement == null){
					if (thatElement != null){
						return false;
					}
				} else if(!thisElement.equals(thatElement)) {
					return false;
				}
			}
			return true;
		} catch(InvalidFieldException e) {
			return false;
		}
	}
	
	
	@Override
	public String toString() {
		try {
		return toString(0,schema.getFields().length-1);
		} catch(InvalidFieldException e){
			throw new RuntimeException(e);
		}
	}
	
	
	public String toString(int minFieldIndex,int maxFieldIndex) throws InvalidFieldException{
		
			StringBuilder b = new StringBuilder("{"); // TODO not optimized,should be cached
			boolean first = true;
			for(int index = minFieldIndex ; index <=maxFieldIndex ; index++) {
				Field field = schema.getField(index);
				String fieldName = field.getName();
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
					if (field.getType() == String.class){
						b.append("\"").append(element.toString()).append("\"");
					} else {
						b.append(element.toString());
					}
				}
			}
			b.append("}");
			return b.toString();
		}

	
}

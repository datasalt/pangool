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
package com.datasalt.pangolin.grouper.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.thrift.TBase;

import com.datasalt.pangolin.commons.Buffer;
import com.datasalt.pangolin.grouper.FieldsDescription;
import com.datasalt.pangolin.grouper.FieldsDescription.Field;
import com.datasalt.pangolin.grouper.Grouper;
import com.datasalt.pangolin.grouper.GrouperException;
import com.datasalt.pangolin.io.Serialization;

/**
 * This is the main serializable {@link WritableComparable} object used in {@link Grouper}. It's configured by a 
 * 
 * @author epalace
 * 
 */
public class Tuple implements WritableComparable<Tuple>,Configurable {
	private Configuration conf;
	
	private Map<String,Object> objects = new HashMap<String,Object>();
	private Set<String> nullObjects = new HashSet<String>();
	private DataOutputBuffer tmpOutputBuffer = new DataOutputBuffer();
	private Buffer tmpInputBuffer = new Buffer();
	private Serialization serialization;
	private FieldsDescription schema;
	private Text text = new Text();

	private Tuple() {
	}
	
	public Tuple(@Nonnull FieldsDescription schema){
		this.schema = schema;
	}

	public FieldsDescription getSchema(){
		return schema;
	}
	
	public void setSerialization(Serialization ser){
		this.serialization = ser;
	}
	
	public void clear(){
		this.objects.clear();
		this.nullObjects.clear();
		populateObjects();
	}
	
	
	public void setSchema(@Nonnull FieldsDescription schema) {
		this.schema = schema;
		clear();
	}
	
	private void populateObjects(){
		for (int i=0; i < schema.getFields().length ; i++){
			Class type = schema.getFields()[i].getType();
			String name = schema.getFields()[i].getName();
			if (type == Integer.class || type == VIntWritable.class){
				objects.put(name,0);
			} else if (type == Long.class || type == VLongWritable.class){
				objects.put(name,0l);
			} else if(type == String.class){
				objects.put(name,"");
			} else if(type == Boolean.class){
				objects.put(name,false);
			} else if(type == Float.class){
				objects.put(name,0.f);
			} else if(type == Double.class){
				objects.put(name,0.0);
			} else {
				Object object = ReflectionUtils.newInstance(type, conf);
				objects.put(name,object);
			}
		}
	}
	
	/**
	 * Thrown when a field is not present in schema
	 * 
	 *
	 */
	public static class NoSuchFieldException extends GrouperException {
    private static final long serialVersionUID = 1L;

		public NoSuchFieldException(String s,Throwable e) {
			super(s,e);
		}
		
		public NoSuchFieldException(String s) {
			super(s);
		}
		
		public NoSuchFieldException(Throwable e) {
			super(e);
		}
	}
	
	private Object getField(String fieldName) throws NoSuchFieldException {
		if (!this.schema.containsFieldName(fieldName)){
			throw new NoSuchFieldException("Field " + fieldName + " not in schema");
		}
		
		if (nullObjects.contains(fieldName)){
			return null;
		} else {
			return objects.get(fieldName);
		}
	}
	
	private void setField(String fieldName,Object value) throws NoSuchFieldException {
		if (!this.schema.containsFieldName(fieldName)){
			throw new NoSuchFieldException("Field \"" + fieldName + "\" not in schema");
		}
		
		if (value == null){
			nullObjects.add(fieldName);
		} else {
			nullObjects.remove(fieldName);
			objects.put(fieldName,value);
		}
	}
	
	
	
	public int getInt(String fieldName) throws NoSuchFieldException {
		return (Integer)getField(fieldName);
	}
	
	public long getLong(String fieldName) throws NoSuchFieldException {
		return (Long)getField(fieldName);
	}
	
	public float getFloat(String fieldName) throws NoSuchFieldException {
		return (Float)getField(fieldName);
	}
	
	public double getDouble(String fieldName) throws NoSuchFieldException {
		return (Double)getField(fieldName);
	}
	
	public String getString(String fieldName) throws NoSuchFieldException {
		return (String)getField(fieldName);
	}
	
	public Object getObject(String fieldName) throws NoSuchFieldException {
		return getField(fieldName);
	}
	
	
	public void setInt(String fieldName, int value) throws NoSuchFieldException {
		setField(fieldName,value);
	}
	
	public void setString(String fieldName,String value) throws NoSuchFieldException {
		setField(fieldName,value);
	}
	
	public void setLong(String fieldName,long value) throws NoSuchFieldException {
		setField(fieldName,value);
	}
	
	public void setFloat(String fieldName,float value) throws NoSuchFieldException {
		setField(fieldName,value);
	}
	
	public void setDouble(String fieldName,double value) throws NoSuchFieldException {
		setField(fieldName,value);
	}
	
	public void setBoolean(String fieldName,boolean value) throws NoSuchFieldException {
		setField(fieldName,value);
	}
	
	public void setObject(String fieldName,Object object) throws NoSuchFieldException {
		setField(fieldName,object);
	}
	
	public void setThriftObject(String fieldName,TBase value) throws NoSuchFieldException{
		setField(fieldName,value);
	}
	
	

	@Override
	public void write(DataOutput output) throws IOException {
		for (int numField = 0; numField < schema.getFields().length; numField++) {
			Class fieldType = schema.getFields()[numField].getType();
			if (fieldType == VIntWritable.class) {
				WritableUtils.writeVInt(output, (Integer) objects.get(numField));
			} else if (fieldType == VLongWritable.class) {
				WritableUtils.writeVLong(output, (Long) objects.get(numField));
			} else if (fieldType == Integer.class){ 
			  output.writeInt((Integer)objects.get(numField));
			} else if (fieldType == Long.class){
				output.writeLong((Long)objects.get(numField));
			} else if (fieldType == Double.class) {
				output.writeDouble((Double) objects.get(numField));
			} else if (fieldType == Float.class) {
				output.writeFloat((Float) objects.get(numField));
			} else if (fieldType == String.class) {
				text.set((String)objects.get(numField));
				text.write(output);
			}	else if (fieldType == Boolean.class) {
				output.writeBoolean((Boolean)objects.get(numField));
			} else {
				Object object = objects.get(numField);
				if (object == null){
					WritableUtils.writeVInt(output,0);
				} else {
					tmpOutputBuffer.reset();
					serialization.ser(object,tmpOutputBuffer);
					WritableUtils.writeVInt(output,tmpOutputBuffer.getLength());
					output.write(tmpOutputBuffer.getData(),0,tmpOutputBuffer.getLength());
				}
			}
		}
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		for (int i =0 ; i < schema.getFields().length ; i++) {
			Class<?> fieldType = schema.getFields()[i].getType();
			String name = schema.getFields()[i].getName();
			if (fieldType == VIntWritable.class) {
				objects.put(name,WritableUtils.readVInt(input));
			} else if (fieldType == VLongWritable.class) {
				objects.put(name,WritableUtils.readVLong(input));
			} else if (fieldType == Integer.class){
				objects.put(name,input.readInt());
			} else if (fieldType == Long.class){
				objects.put(name, input.readLong());
			}	else if (fieldType == Double.class) {
				objects.put(name, input.readDouble());
			} else if (fieldType == Float.class) {
				objects.put(name, input.readFloat());
			} else if (fieldType == String.class) {
				text.readFields(input);
				objects.put(name, text.toString());
			} else if (fieldType == Boolean.class) {
				objects.put(name, input.readBoolean());
			} else {
				int size =WritableUtils.readVInt(input);
				if (size != 0){
					tmpInputBuffer.setSize(size);
					input.readFully(tmpInputBuffer.getBytes(),0,size);
					serialization.deser(objects.get(name),tmpInputBuffer.getBytes(),0,size);
				} else {
					// TODO this should mark as null the object, but keeping the cached instance.
				}
			}
		}
	}
	
	public void deepCopyFrom(Tuple tuple) {
		setSchema(tuple.getSchema());
		try{
			for(Field field : schema.getFields()) {
				String fieldName = field.getName();
				// TODO make deep copy !!!!
				setField(fieldName, tuple.getField(fieldName));

			}
		} catch(NoSuchFieldException e) {
			// shouldn't occur
			throw new RuntimeException(e);
		}
	}

	/**
	 * Calculates a combinated hashCode using the specified fields.
	 * @param fields
	 * @return
	 * @throws NoSuchFieldException
	 */
	public int partialHashCode(String[] fields) throws NoSuchFieldException {
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
  public int compareTo(Tuple that) {
		//TODO this method makes sense ? Mark it like NotImplemented ?
		try{
		if (!this.schema.equals(that.schema)){
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
		} catch(NoSuchFieldException e){
			throw new RuntimeException(e);
		}
  }
	
	private int compareObjects(Object element1, Object element2 ){

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
		if (!(tuple2 instanceof Tuple)){
			return false;
		}
		
		try {
			for(Field field : schema.getFields()) {
				String fieldName = field.getName();
				Object thisElement = getField(fieldName);
				Object thatElement = ((Tuple) tuple2).getField(fieldName);
				if (thisElement == null){
					if (thatElement != null){
						return false;
					}
				} else if(!thisElement.equals(thatElement)) {
					return false;
				}
			}
			return true;
		} catch(NoSuchFieldException e) {
			return false;
		}
	}
	
	
	@Override
	public String toString() {
		try {
			StringBuilder b = new StringBuilder("{"); // TODO not optimized,should be cached
			boolean first = true;
			for(Field field : schema.getFields()) {
				String fieldName = field.getName();
				Object element = getField(fieldName);
				if(!first) {
					b.append(",");
				} else {
					first = false;
				}
				String st = (element == null) ? "null" : element.toString();
				b.append(fieldName).append(":").append(st);
			}
			b.append("}");
			return b.toString();
		} catch(NoSuchFieldException e) {
			throw new RuntimeException(e);
		}
	}
}

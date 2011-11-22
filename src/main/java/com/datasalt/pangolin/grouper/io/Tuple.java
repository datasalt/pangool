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
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

import javax.annotation.Nonnull;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.contrib.serialization.thrift.ThriftSerialization;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.thrift.TBase;

import com.datasalt.pangolin.commons.Buffer;
import com.datasalt.pangolin.grouper.GrouperException;
import com.datasalt.pangolin.grouper.Schema;
import com.datasalt.pangolin.io.Serialization;

/**
 * This is the main serializable {@link WritableComparable} object used in {@link Grouper}. It's configured by a 
 * 
 * @author epalace
 * 
 */
public class Tuple implements WritableComparable<Tuple>,Configurable {
	//private ThriftSerialization thriftSerialization = 
	private Configuration conf;
	private Comparable[] objects= new Comparable[0];
	private DataOutputBuffer tmpOutputBuffer = new DataOutputBuffer();
	private Buffer tmpInputBuffer = new Buffer();
	private Serialization serialization;
	//private ThriftSerialization thriftSerialization = new ThriftSerialization();
	private Schema schema;
	private Text text = new Text();

	public Tuple() {
	}

	public void setSerialization(Serialization ser){
		this.serialization = ser;
	}
	
	
	public Comparable get(int index){
		return objects[index];
	}
	
	public void setSchema(@Nonnull Schema schema) {
		this.schema = schema;
		// TODO this should erase previous state ?
		this.objects = new Comparable[schema.getFields().length];
		populateObjects();
		
	}
	
	private void populateObjects(){
		for (int i=0; i < schema.getFields().length ; i++){
			Class type = schema.getFields()[i].getType();
			if (type == Integer.class || type == VIntWritable.class){
				objects[i]=0;
			} else if (type == Long.class || type == VLongWritable.class){
				objects[i]=0l;
			} else if(type == String.class){
				objects[i]="";
			} else if(type == Boolean.class){
				objects[i]=true;
			} else if(type == Float.class){
				objects[i]=0.f;
			} else if(type == Double.class){
				objects[i]=0.0;
			} else if(type == Boolean.class){
				objects[i] = false;
			} else {
				objects[i] = (Comparable)ReflectionUtils.newInstance(type, conf);
			}
		}
	}
	
	
	public static class NoSuchFieldException extends GrouperException {
		public NoSuchFieldException(String s,Throwable e) {
			super(s,e);
		}
	}
	
	public Object getField(String fieldName) throws NoSuchFieldException {
		int index = this.schema.getIndexByFieldName(fieldName);
		try{
			return objects[index];
		} catch(IndexOutOfBoundsException e){
			throw new NoSuchFieldException("Field \"" + fieldName + "\" not in schema",e);
		}
	}
	

	public void setField(String fieldName,Comparable value) throws NoSuchFieldException {
		int index = this.schema.getIndexByFieldName(fieldName);
		try{
		objects[index] = value;
		} catch(IndexOutOfBoundsException e){
			throw new NoSuchFieldException("Field \"" + fieldName + "\" not in schema",e);
		}
	}

	public void setField(int index,Comparable value) {
		this.objects[index] = value;
	}

	@Override
	public void write(DataOutput output) throws IOException {
		for (int numField = 0; numField < schema.getFields().length; numField++) {
			Class fieldType = schema.getFields()[numField].getType();
			if (fieldType == VIntWritable.class) {
				WritableUtils.writeVInt(output, (Integer) objects[numField]);
			} else if (fieldType == VLongWritable.class) {
				WritableUtils.writeVLong(output, (Long) objects[numField]);
			} else if (fieldType == Integer.class){ 
			  output.writeInt((Integer)objects[numField]);
			} else if (fieldType == Long.class){
				output.writeLong((Long)objects[numField]);
			} else if (fieldType == Double.class) {
				output.writeDouble((Double) objects[numField]);
			} else if (fieldType == Float.class) {
				output.writeFloat((Float) objects[numField]);
			} else if (fieldType == String.class) {
				text.set((String)objects[numField]);
				text.write(output);
			}	else if (fieldType == Boolean.class) {
				output.writeBoolean((Boolean)objects[numField]);
			} else {
			//} else if ( TBase.class.isAssignableFrom(fieldType)){
				//TODO should we use serialization from Hadoop here or directly Thrift serializer
				tmpOutputBuffer.reset();
				serialization.ser(objects[numField],tmpOutputBuffer);
				WritableUtils.writeVInt(output,tmpOutputBuffer.getLength());
				output.write(tmpOutputBuffer.getData(),0,tmpOutputBuffer.getLength());
				//TODO output correct exception 
				//throw new RuntimeException("Not implemented fieldType : " + fieldType); 
			}
		}
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		for (int i =0 ; i < schema.getFields().length ; i++) {
			Class<?> fieldType = schema.getFields()[i].getType();
			if (fieldType == VIntWritable.class) {
				setField(i, WritableUtils.readVInt(input));
			} else if (fieldType == VLongWritable.class) {
				setField(i, WritableUtils.readVLong(input));
			} else if (fieldType == Integer.class){
				setField(i,input.readInt());
			} else if (fieldType == Long.class){
				setField(i,input.readLong());
			}	else if (fieldType == Double.class) {
				setField(i, input.readDouble());
			} else if (fieldType == Float.class) {
				setField(i, input.readFloat());
			} else if (fieldType == String.class) {
				text.readFields(input);
				setField(i, text.toString());
			} else if (fieldType == Boolean.class) {
				setField(i,input.readBoolean());
			} else {
			//} else if ( TBase.class.isAssignableFrom(fieldType)){
				int size =WritableUtils.readVInt(input);
				tmpInputBuffer.setSize(size);
				//tmpInputBuffer.setSize(0);
				input.readFully(tmpInputBuffer.getBytes(),0,size);
				
				//serialization.deser(objects[i],(DataInputStream) input);
				serialization.deser(objects[i],tmpInputBuffer.getBytes(),0,size);
				
			//} else {
			//	throw new RuntimeException("Not implemented fieldType :  " + fieldType);
			}
		}
	}
	
	public void set(Tuple tuple) {
		this.schema = tuple.schema;
		
		if (objects == null || objects.length != schema.getFields().length) {
			objects = new Comparable[schema.getFields().length];
		}
		for (int i = 0; i < schema.getFields().length; i++) {
			objects[i] = tuple.objects[i];
		}
	}

		public int partialHashCode(int[] fieldsIndexes) {
		int result = 0;
		for (int fieldIndex : fieldsIndexes) {
			result = result * 31 + objects[fieldIndex].hashCode();
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
				Schema schema =Schema.parse(this.conf);
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
		if (!this.schema.equals(that.schema)){
			throw new RuntimeException("Schemas are different");
		}
		
		for (int i= 0 ; i < this.objects.length ; i++){
			int comparison = this.objects[i].compareTo(that.objects[i]);
			if (comparison != 0){
				return comparison;
			}
		}
		return 0;
  }
	
	@Override
	public boolean equals(Object tuple2){
		if (!(tuple2 instanceof Tuple)){
			return false;
		}
		return this.toString().equals(tuple2.toString());
	}
	
	
	@Override
	public String toString(){
		if (this.objects == null || this.objects.length == 0){
			return "(empty)";
		}
		StringBuilder b = new StringBuilder("{"); //TODO not optimized
		b.append(this.objects[0]);
		for (int i=1 ; i < this.objects.length ; i++){
			b.append(",").append(this.objects[i]);
		}
		b.append("}");
		return b.toString();
	}
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasalt.pangolin.grouper.io.tuple.serialization;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Serializer;

import com.datasalt.pangolin.grouper.Schema;
import com.datasalt.pangolin.grouper.Schema.Field;
import com.datasalt.pangolin.grouper.io.tuple.ITuple;
import com.datasalt.pangolin.io.Serialization;


class TupleSerializer implements Serializer<ITuple> {

	
	private Serialization ser;
	
	private DataOutputStream out;
	private Schema schema;
	private Text text= new Text();
	
  //private Map<String,Enum<?>[]> cachedEnums;
  private DataOutputBuffer tmpOutputBuffer = new DataOutputBuffer();
	

  public void open(OutputStream out) {
  	this.out = new DataOutputStream(out);
  }

  public void serialize(ITuple tuple) throws IOException {
  	write(tuple,out);
  }

  public void close() throws IOException {
  	this.out.close();
  }
  
  void setSchema(Schema schema){
  	this.schema = schema;
  	//cachedEnums = TupleSerialization.cacheEnums(schema);
  }
  
  void setSerialization(Serialization ser){
  	this.ser = ser;
  }
  
  public static int defaultInt(Object o){
  	return (o == null) ? 0: (Integer)o;
  }
  
  public static long defaultLong(Object o){
  	return (o == null) ? 0l: (Long)o;
  }
  
  public static float defaultFloat(Object o){
  	return (o == null) ? 0f: (Float)o;
  }
  
  public static double defaultDouble(Object o){
  	return (o == null) ? 0.0: (Double)o;
  }
  
  public static boolean defaultBoolean(Object o){
  	return (o == null) ? false: (Boolean)o;
  }
  
  private String defaultString(Object o){
  	return (o == null) ? "": (String)o;
  }
  
	public void write(ITuple tuple,DataOutput output) throws IOException {
		int presentFields = 0;
		for (Field field : schema.getFields()) {
			String fieldName = field.getName();
			Class<?> fieldType = field.getType();
			Object element = tuple.getObject(fieldName);
			if (element != null) {
				presentFields++;
			} 
			try {
				if (fieldType == VIntWritable.class) {
					WritableUtils.writeVInt(output, defaultInt(element));
				} else if (fieldType == VLongWritable.class) {
					WritableUtils.writeVLong(output, defaultLong(element));
				} else if (fieldType == Integer.class) {
					output.writeInt(defaultInt(element));
				} else if (fieldType == Long.class) {
					output.writeLong(defaultLong(element));
				} else if (fieldType == Double.class) {
					output.writeDouble(defaultDouble(element));
				} else if (fieldType == Float.class) {
					output.writeFloat(defaultFloat(element));
				} else if (fieldType == String.class) {
					text.set(defaultString(element));
					text.write(output);
				} else if (fieldType == Boolean.class) {
					output.write(defaultBoolean(element) ? 1 : 0);
				} else if (fieldType.isEnum()) {
					Enum<?> e = (Enum<?>) element;
					if (e.getClass() != fieldType){
						throw new IOException("Field '" + fieldName + "' contains '" + element
						+ "' which is " + element.getClass().getName()
						+ ".The expected type is " + fieldType.getName());
					}
					WritableUtils.writeVInt(output, e.ordinal());
				} else {
					if (element == null) {
						WritableUtils.writeVInt(output, 0);
					} else {
						tmpOutputBuffer.reset();
						ser.ser(element, tmpOutputBuffer);
						WritableUtils.writeVInt(output, tmpOutputBuffer.getLength());
						output.write(tmpOutputBuffer.getData(), 0,
								tmpOutputBuffer.getLength());
					}
				}
			} catch (ClassCastException e) {
				throw new IOException("Field '" + fieldName + "' contains '" + element
						+ "' which is " + element.getClass().getName()
						+ ".The expected type is " + fieldType.getName());
			}
		} //end for
		
		if (tuple.size() > presentFields){
			raiseExceptionWrongFields(tuple);
		}
		
	}
	
	private void raiseExceptionWrongFields(ITuple tuple) throws IOException{
		List<String> wrongFields = new ArrayList<String>();
		for (String field : tuple.keySet()){
			if (!schema.containsFieldName(field)){
				wrongFields.add(field);
			}
		}
		String fieldsConcated = concat(wrongFields,",");
		throw new IOException("Tuple contains fields that don't belong to schema " + fieldsConcated + ". Schema:"+schema);
		
	}
	
	private String concat(List<String> list,String separator){
		if (list == null || list.isEmpty()){
			return "";
		} else {
			StringBuilder b = new StringBuilder();
			b.append(list.get(0));
			for (int i=1 ; i < list.size(); i++){
				b.append(separator).append(list.get(i));
			}
			return b.toString();
		}
		
	}
	

}

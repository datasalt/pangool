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
package com.datasalt.pangool.io.tuple;

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

import com.datasalt.pangolin.io.Serialization;
import com.datasalt.pangool.PangoolConfig;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.Schema.Field;

class SourcedTupleSerializer implements Serializer<ISourcedTuple> {

	private Serialization ser;
	
	private DataOutputStream out;
	private PangoolConfig pangoolConfig;
	private Text text= new Text();
	
  private DataOutputBuffer tmpOutputBuffer = new DataOutputBuffer();
	
  SourcedTupleSerializer(Serialization ser,PangoolConfig pangoolConfig){
		this.pangoolConfig = pangoolConfig;
		this.ser = ser;
	}
	
  public void open(OutputStream out) {
  	this.out = new DataOutputStream(out);
  }

  public void serialize(ISourcedTuple tuple) throws IOException {
  	write(tuple,out);
  }

  public void close() throws IOException {
  	this.out.close();
  }
  
  private static void throwIOIfNull(String field,Object element) throws IOException {
  	if (element == null){
  		throw new IOException("Field '" + field + "' can't be null");
  	}
  } 
  
  public void write(ISourcedTuple tuple,DataOutput output) throws IOException {
  	// First we write common schema
  	Schema commonSchema = pangoolConfig.getCommonOrderedSchema();
  	int presentFields = 0;
  	presentFields += write(commonSchema,tuple,output);
  	
  	// Now we write specific part if needed. 
  	presentFields += writeSpecificPart(tuple, output);
  	
  	if (tuple.size() > presentFields ){
  		Schema schema = pangoolConfig.getSchemes().get(tuple.getInt(Field.SOURCE_ID_FIELD_NAME));
  		raiseExceptionWrongFields(schema,tuple);
  	}  	
  }
  
  /**
   * Writes the specific part of the tuple. Return number of present fields  
   */
  public int writeSpecificPart(ISourcedTuple tuple, DataOutput output) throws IOException {
  	int numSourcesDefined = pangoolConfig.getSchemes().size();  	

  	// If only one schema defined as source, then there are not specific part.
  	if (numSourcesDefined == 1) {
  		return 0;
  	}
  	
  	if (!pangoolConfig.getSchemes().containsKey(tuple.getInt(Field.SOURCE_ID_FIELD_NAME))){
  		throw new IOException(
  				"Tuple " + Field.SOURCE_ID_FIELD_NAME + " not present but more than one source present. " +
  				"Sources: " + pangoolConfig.getSchemes() +  " actualSource=" + tuple.getInt(Field.SOURCE_ID_FIELD_NAME));
  	}

		Schema schema = pangoolConfig.getSpecificOrderedSchemas().get(tuple.getInt(Field.SOURCE_ID_FIELD_NAME));
  	return write(schema,tuple,output);  	
  }
  
	public int write(Schema schema,ISourcedTuple tuple,DataOutput output) throws IOException {
		int presentFields = 0;
		for (Field field : schema.getFields()) {
			String fieldName = field.getName();
			if (fieldName == Field.SOURCE_ID_FIELD_NAME){
				WritableUtils.writeVInt(output, tuple.getInt(Field.SOURCE_ID_FIELD_NAME));
				presentFields++;
				continue;
			}
			Class<?> fieldType = field.getType();
			Object element = tuple.getObject(fieldName);
			if (element != null) {
				presentFields++;
			} 
			try {
				if (fieldType == VIntWritable.class) {
					throwIOIfNull(fieldName,element);
					WritableUtils.writeVInt(output, (Integer)element);
				} else if (fieldType == VLongWritable.class) {
					throwIOIfNull(fieldName,element);
					WritableUtils.writeVLong(output, (Long)element);
				} else if (fieldType == Integer.class) {
					throwIOIfNull(fieldName,element);
					output.writeInt((Integer)element);
				} else if (fieldType == Long.class) {
					throwIOIfNull(fieldName,element);
					output.writeLong((Long)element);
				} else if (fieldType == Double.class) {
					throwIOIfNull(fieldName,element);
					output.writeDouble((Double)element);
				} else if (fieldType == Float.class) {
					throwIOIfNull(fieldName,element);
					output.writeFloat((Float)element);
				} else if (fieldType == String.class) {
					if (element == null){
						element = "";
					} 
					text.set((String)element);
					text.write(output);
				} else if (fieldType == Boolean.class) {
					throwIOIfNull(fieldName,element);
					output.write((Boolean)element ? 1 : 0);
				} else if (fieldType.isEnum()) {
					Enum<?> e = (Enum<?>) element;
					throwIOIfNull(fieldName,element);
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
		
		return presentFields;		
	}
	
	private void raiseExceptionWrongFields(Schema schema,ISourcedTuple tuple) throws IOException{
		List<String> wrongFields = new ArrayList<String>();
		for (String field : tuple.keySet()){
			if (!schema.containsFieldName(field)){
				wrongFields.add(field);
			}
		}
		String fieldsConcated = concat(wrongFields,",");
		throw new IOException("Tuple with source " + tuple.getInt(Field.SOURCE_ID_FIELD_NAME) + " contains fields that don't belong to schema '" + fieldsConcated + "'.\nSchema:"+schema);	
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

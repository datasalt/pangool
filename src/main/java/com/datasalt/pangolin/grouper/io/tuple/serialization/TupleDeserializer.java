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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.util.ReflectionUtils;

import com.datasalt.pangolin.commons.Buffer;
import com.datasalt.pangolin.grouper.Schema;
import com.datasalt.pangolin.grouper.io.tuple.ITuple;
import com.datasalt.pangolin.grouper.io.tuple.Tuple;
import com.datasalt.pangolin.io.Serialization;


class TupleDeserializer implements Deserializer {

	private Schema schema;
	private DataInputStream in;
	private Text text = new Text();
	private Serialization ser;
	private Map<String,Enum<?>[]> cachedEnums = new HashMap<String,Enum<?>[]>();
	
	private Buffer tmpInputBuffer = new Buffer();
	//private 
	
	
	TupleDeserializer(Serialization ser,Schema schema){
		this.schema = schema;
		this.ser = ser;
		this.cachedEnums = TupleSerialization.cacheEnums(schema);
	}
	
	
	@Override
	public void open(InputStream in) throws IOException {
		this.in = new DataInputStream(in);
		//this.in = in;
		
		
	}

	@Override
	public ITuple deserialize(Object t) throws IOException {
		
		if (t == null) {
			t = ReflectionUtils.newInstance(Tuple.class, null);
		}
		
//		if (result instanceof Tuple){
//			((Tuple)result).swapInstances();
//		}
		
		readFields((ITuple)t, in);
		return (ITuple)t;
	}
	
	
	
	public void readFields(ITuple tuple,DataInput input) throws IOException {
		for (int i =0 ; i < schema.getFields().length ; i++) {
			Class<?> fieldType = schema.getFields()[i].getType();
			String name = schema.getFields()[i].getName();
			if (fieldType == VIntWritable.class) {
				tuple.setInt(name,WritableUtils.readVInt(input));
			} else if (fieldType == VLongWritable.class) {
				tuple.setLong(name,WritableUtils.readVLong(input));
			} else if (fieldType == Integer.class){
				tuple.setInt(name,input.readInt());
			} else if (fieldType == Long.class){
				tuple.setLong(name, input.readLong());
			}	else if (fieldType == Double.class) {
				tuple.setDouble(name, input.readDouble());
			} else if (fieldType == Float.class) {
				tuple.setFloat(name, input.readFloat());
			} else if (fieldType == String.class) {
				text.readFields(input);
				tuple.setString(name, text.toString());
			} else if (fieldType == Boolean.class) {
				byte b = input.readByte();
				tuple.setBoolean(name, (b != 0));
			} else if (fieldType.isEnum()){
				int ordinal = WritableUtils.readVInt(input);
				try{
					Enum<?>[] enums = cachedEnums.get(name);
					if (enums == null){
						throw new IOException("Field "+ name + " is not a enum type");
					}
					tuple.setObject(name,enums[ordinal]);
				} catch (ArrayIndexOutOfBoundsException e){
					throw new RuntimeException(e);
				}
			} else {
				int size =WritableUtils.readVInt(input);
				if (size != 0){
					tmpInputBuffer.setSize(size);
					input.readFully(tmpInputBuffer.getBytes(),0,size);
					if (tuple.getObject(name) == null){
						tuple.setObject(name,ReflectionUtils.newInstance(fieldType, null));
					}
					
					Object ob = ser.deser(tuple.getObject(name),tmpInputBuffer.getBytes(),0,size);
					tuple.setObject(name, ob);
					
				} else {
					tuple.setObject(name, null);
				}
			}
		}
	}

	@Override
	public void close() throws IOException {
		in.close();
		
	}


  
//	void setSchema(Schema schema){
//		this.schema = schema;
//		
//	}
//	
//	void setSerialization(Serialization ser){
//		this.ser = ser;
//	}
 

}

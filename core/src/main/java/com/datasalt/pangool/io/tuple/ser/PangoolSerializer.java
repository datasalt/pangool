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
package com.datasalt.pangool.io.tuple.ser;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Serializer;

import com.datasalt.pangool.CoGrouperConfig;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.Schema.Field;
import com.datasalt.pangool.io.Serialization;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.io.tuple.ITupleInternal;
import com.datasalt.pangool.io.tuple.DatumWrapper;

public class PangoolSerializer implements Serializer<DatumWrapper<ITuple>> {

	private Serialization ser;
	
	private DataOutputStream out;
	private CoGrouperConfig coGrouperConfig;
	private Text text = new Text();
	private static final Text EMPTY_TEXT = new Text("");
	
	private DataOutputBuffer tmpOutputBuffer = new DataOutputBuffer();

	PangoolSerializer(Serialization ser, CoGrouperConfig grouperConfig) {
		this.coGrouperConfig = grouperConfig;
		this.ser = ser;
	}

	public void open(OutputStream out) {
		this.out = new DataOutputStream(out);
	}

	public void serialize(DatumWrapper<ITuple> wrapper) throws IOException {
		ITuple tuple = wrapper.currentDatum();
		//TODO check that schema is valid
		//Schema may not match the source id 
		write(tuple.getSchema(),tuple,out);

	}

	public void close() throws IOException {
		this.out.close();
	}

	private void write(Schema schema, ITuple tuple, DataOutput output) throws IOException {
		for(int i=0; i < schema.getFields().size(); i++) {
			Field field = schema.getField(i);
			String fieldName = field.name();
			Class<?> fieldType = field.type();
			Object element;
			element = tuple.get(i);
			try {
				if(fieldType == VIntWritable.class) {
					WritableUtils.writeVInt(output, (Integer) element);
				} else if(fieldType == VLongWritable.class) {
					WritableUtils.writeVLong(output, (Long) element);
				} else if(fieldType == Integer.class) {
					output.writeInt((Integer) element);
				} else if(fieldType == Long.class) {
					output.writeLong((Long) element);
				} else if(fieldType == Double.class) {
					output.writeDouble((Double) element);
				} else if(fieldType == Float.class) {
					output.writeFloat((Float) element);
				} else if(fieldType == String.class) {
					if (element == null){
						EMPTY_TEXT.write(output);
					} else if (element instanceof Text){
						((Text)element).write(output);
					} else if (element instanceof String){
						text.set((String)element);
						text.write(output);
					} 
				} else if(fieldType == Boolean.class) {
					output.write((Boolean) element ? 1 : 0);
				} else if(fieldType.isEnum()) {
					Enum<?> e = (Enum<?>) element;
					if(e.getClass() != fieldType) {
						throw new IOException("Field '" + fieldName + "' contains '" + element + "' which is "
						    + element.getClass().getName() + ".The expected type is " + fieldType.getName());
					}
					WritableUtils.writeVInt(output, e.ordinal());
				} else {
					if(element == null) {
						WritableUtils.writeVInt(output, 0);
					} else {
						tmpOutputBuffer.reset();
						ser.ser(element, tmpOutputBuffer);
						WritableUtils.writeVInt(output, tmpOutputBuffer.getLength());
						output.write(tmpOutputBuffer.getData(), 0, tmpOutputBuffer.getLength());
					}
				}
			} catch(ClassCastException e) {
				throw new IOException("Field '" + fieldName + "' contains '" + element + "' which is "
				    + element.getClass().getName() + ".The expected type is " + fieldType.getName());
			} // end for
		} 
		
	}
}

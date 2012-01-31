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

import com.datasalt.pangool.CoGrouperConfig;
import com.datasalt.pangool.Schema;
import com.datasalt.pangool.Schema.Field;
import com.datasalt.pangool.io.Buffer;
import com.datasalt.pangool.io.Serialization;
import com.datasalt.pangool.io.tuple.DoubleBufferedTuple;
import com.datasalt.pangool.io.tuple.ITupleInternal;

class TupleInternalDeserializer implements Deserializer<ITupleInternal> {

	private CoGrouperConfig coGrouperConf;
	private DataInputStream in;
	private Text text = new Text();
	private Serialization ser;
	private Map<String, Enum<?>[]> cachedEnums = new HashMap<String, Enum<?>[]>();

	private Buffer tmpInputBuffer = new Buffer();
	private Class<? extends ITupleInternal> instanceClazz;

	TupleInternalDeserializer(Serialization ser, CoGrouperConfig pangoolConfig,
	    Class<? extends ITupleInternal> instanceClass) {
		this.coGrouperConf = pangoolConfig;
		this.ser = ser;
		this.cachedEnums = TupleInternalSerialization.getEnums(pangoolConfig);
		this.instanceClazz = instanceClass;
	}

	@Override
	public void open(InputStream in) throws IOException {
		this.in = new DataInputStream(in);
	}

	@Override
	public ITupleInternal deserialize(ITupleInternal t) throws IOException {
		Schema commonSchema = coGrouperConf.getCommonOrderedSchema();
		int tupleSize = commonSchema.getFields().length;
		if(t == null) {
			t = ReflectionUtils.newInstance(instanceClazz, null);
		}
		if(t instanceof DoubleBufferedTuple) {
			((DoubleBufferedTuple) t).swapInstances();
		}
		t.clear();
		if(t.getArray() == null) {
			t.setArray(new Object[tupleSize]);
		}
		int sourceId = readFields(commonSchema, t, 0, in);
		if(coGrouperConf.getnSchemas() > 1) {
			// Expand / shrink backed tuple array when needed
			Schema specificSchema = coGrouperConf.getSpecificOrderedSchema(sourceId);
			tupleSize += specificSchema.getFields().length;
			Object[] newArray = new Object[tupleSize];
			int sizeToCopy = t.getArray().length;
			if(tupleSize < sizeToCopy) {
				sizeToCopy = tupleSize;
			}
			System.arraycopy((Object)t.getArray(), 0, (Object)newArray, 0, sizeToCopy);
			t.setArray(newArray);
			readFields(specificSchema, t, commonSchema.getFields().length, in);
		}
		return t;
	}

	public int readFields(Schema schema, ITupleInternal tuple, int index, DataInput input) throws IOException {
		Object[] deSer = tuple.getArray();
		int sourceId = 0;
		for(int i = 0; i < schema.getFields().length; i++) {
			Class<?> fieldType = schema.getField(i).getType();
			String fieldName = schema.getField(i).getName();
			if(Field.SOURCE_ID_FIELD_NAME.equals(fieldName)) {
				sourceId = WritableUtils.readVInt(input);
				deSer[index] = sourceId;
			} else if(fieldType == VIntWritable.class) {
				deSer[index] = WritableUtils.readVInt(input);
			} else if(fieldType == VLongWritable.class) {
				deSer[index] = WritableUtils.readVLong(input);
			} else if(fieldType == Integer.class) {
				deSer[index] = input.readInt();
			} else if(fieldType == Long.class) {
				deSer[index] = input.readLong();
			} else if(fieldType == Double.class) {
				deSer[index] = input.readDouble();
			} else if(fieldType == Float.class) {
				deSer[index] = input.readFloat();
			} else if(fieldType == String.class) {
				text.readFields(input);
				byte[] bytes = text.getBytes();
			  byte[] newBytes = new byte[text.getLength()];
		    System.arraycopy(bytes, 0, newBytes, 0, text.getLength());
				deSer[index] = newBytes;
			} else if(fieldType == Boolean.class) {
				byte b = input.readByte();
				deSer[index] = (b != 0);
			} else if(fieldType.isEnum()) {
				int ordinal = WritableUtils.readVInt(input);
				try {
					Enum<?>[] enums = cachedEnums.get(fieldName);
					if(enums == null) {
						throw new IOException("Field " + fieldName + " is not a enum type");
					}
					deSer[index] = enums[ordinal];
				} catch(ArrayIndexOutOfBoundsException e) {
					throw new RuntimeException(e);
				}
			} else {
				int size = WritableUtils.readVInt(input);
				if(size != 0) {
					tmpInputBuffer.setSize(size);
					input.readFully(tmpInputBuffer.getBytes(), 0, size);
					if(tuple.getObject(index) == null) {
						tuple.setObject(index, ReflectionUtils.newInstance(fieldType, null));
					}
					Object ob = ser.deser(tuple.getObject(index), tmpInputBuffer.getBytes(), 0, size);
					tuple.setObject(index, ob);
				}
			} // end for
			index++;
		}
		return sourceId;
	}

	@Override
	public void close() throws IOException {
		in.close();
	}
}

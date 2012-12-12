/**
 * Copyright [2012] [Datasalt Systems S.L.]
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
package com.datasalt.pangool.tuplemr.serialization;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.util.ReflectionUtils;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Utf8;
import com.datasalt.pangool.serialization.HadoopSerialization;
import com.datasalt.pangool.tuplemr.SerializationInfo;
import com.datasalt.pangool.utils.Buffer;

/**
 * This Deserializer holds all the baseline code for deserializing Tuples. It is used by the more complex {@link TupleDeserializer}.
 * It is also used by a stateful Tuple field serializer {@link TupleFieldSerialization}. 
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class SimpleTupleDeserializer implements Deserializer<ITuple> {

	private DataInputStream input;
	private final HadoopSerialization ser;
	private final Buffer tmpInputBuffer = new Buffer();
	private final Configuration conf;

	private Deserializer[] deserializers; 
	private Schema schemaToDeserialize;
	
	public SimpleTupleDeserializer(HadoopSerialization ser, Configuration conf) {
		this.ser = ser;
		this.conf = conf;
	}

  /**
   * Constructor where you must include the scheme. Mandatory if you are
   * using custom stateful serializers.
   */
	public SimpleTupleDeserializer(Schema schemaToDeserialize, HadoopSerialization ser, Configuration conf) {
		this(ser, conf);
		this.schemaToDeserialize = schemaToDeserialize;
		deserializers = SerializationInfo.getDeserializers(schemaToDeserialize, conf);
	}
	
	@Override
	public void close() throws IOException {
		input.close();
	}

	@Override
	public ITuple deserialize(ITuple tuple) throws IOException {
		if(tuple == null) {
			tuple = new Tuple(schemaToDeserialize);
		}
		readFields(tuple, deserializers);
		return tuple;
	}

	@Override
	public void open(InputStream input) throws IOException {
		if(input instanceof DataInputStream) {
			this.input = (DataInputStream) input;
		} else {
			this.input = new DataInputStream(input);
		}
	}

	public void readFields(ITuple tuple, Deserializer[] customDeserializers) throws IOException {
		Schema schema = tuple.getSchema();
		for(int index = 0; index < schema.getFields().size(); index++) {
			Deserializer customDeser = customDeserializers[index];
			Field field = schema.getField(index);
			switch(field.getType()) {
			case INT:
				tuple.set(index, WritableUtils.readVInt(input));
				break;
			case LONG:
				tuple.set(index, WritableUtils.readVLong(input));
				break;
			case DOUBLE:
				tuple.set(index, input.readDouble());
				break;
			case FLOAT:
				tuple.set(index, input.readFloat());
				break;
			case STRING:
				readUtf8(input, tuple, index);
				break;
			case BOOLEAN:
				byte b = input.readByte();
				tuple.set(index, (b != 0));
				break;
			case ENUM:
				readEnum(input, tuple, field.getObjectClass(), index);
				break;
			case BYTES:
				readBytes(input, tuple, index);
				break;
			case OBJECT:
				readCustomObject(input, tuple, field.getObjectClass(), index, customDeser);
				break;
			default:
				throw new IOException("Not supported type:" + field.getType());
			}
		}
	}

	protected void readUtf8(DataInputStream input, ITuple tuple, int index) throws IOException {
    Object t = tuple.get(index);
    if(t == null || !(t instanceof Utf8)) {
      t = new Utf8();
      tuple.set(index, t);
    }
    ((Utf8) t).readFields(input);

  }

	protected void readCustomObject(DataInputStream input, ITuple tuple, Class<?> expectedType, int index,
	    Deserializer customDeser) throws IOException {
		int size = WritableUtils.readVInt(input);
		if(size >= 0) {
			Object object = tuple.get(index);
			if(customDeser != null) {
				customDeser.open(input);
				object = customDeser.deserialize(object);
				customDeser.close();
				tuple.set(index, object);
			} else {
				if(object == null) {
					tuple.set(index, ReflectionUtils.newInstance(expectedType, conf));
				}
				tmpInputBuffer.setSize(size);
				input.readFully(tmpInputBuffer.getBytes(), 0, size);
				Object ob = ser.deser(tuple.get(index), tmpInputBuffer.getBytes(), 0, size);
				tuple.set(index, ob);
			}
		} else {
			throw new IOException("Error deserializing, custom object serialized with negative length : " + size);
		}
	}

	public void readBytes(DataInputStream input, ITuple tuple, int index) throws IOException {
		int length = WritableUtils.readVInt(input);
		ByteBuffer old = (ByteBuffer) tuple.get(index);
		ByteBuffer result;
		if(old != null && length <= old.capacity()) {
			result = old;
			result.clear();
		} else {
			result = ByteBuffer.allocate(length);
			tuple.set(index, result);
		}
		input.readFully(result.array(), result.position(), length);
		result.limit(length);
	}

	public DataInputStream getInput() {
  	return input;
  }

	protected void readEnum(DataInputStream input, ITuple tuple, Class<?> fieldType, int index) throws IOException {
		int ordinal = WritableUtils.readVInt(input);
		try {
			Object[] enums = fieldType.getEnumConstants();
			tuple.set(index, enums[ordinal]);
		} catch(ArrayIndexOutOfBoundsException e) {
			throw new IOException("Ordinal index out of bounds for " + fieldType + " ordinal=" + ordinal);
		}
	}
}

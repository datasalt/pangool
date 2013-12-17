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
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.util.ReflectionUtils;

import com.datasalt.pangool.io.BitField;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.io.TupleFile;
import com.datasalt.pangool.io.Utf8;
import com.datasalt.pangool.serialization.HadoopSerialization;
import com.datasalt.pangool.tuplemr.SerializationInfo;
import com.datasalt.pangool.utils.Buffer;

/**
 * This Deserializer holds all the baseline code for deserializing Tuples. It is used by the more complex
 * {@link TupleDeserializer}. It is also used by a stateful Tuple field serializer {@link TupleFieldSerialization} and
 * finally it is also used by the stateful {@link TupleFile}.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class SimpleTupleDeserializer implements Deserializer<ITuple> {

	private DataInputStream input;
	private final HadoopSerialization ser;
	private final Buffer tmpInputBuffer = new Buffer();
	private final BitField nullsRelative = new BitField();
	private final FlagsField nullsAbsolute = new FlagsField();
	private final Configuration conf;

	private Deserializer[] deserializers;
	private Schema readSchema = null, targetSchema = null;
	private int[] backwardsCompatibiltyLookupVector = null;

	// Constant that indicates a field of a Tuple is not being used
	private final static int UNUSED = -1;
	
	/**
	 * Package-visibility constructor used by {@link TupleDeserializer} . For efficiency, a Schema is not pre-configured.
	 * Read schemas are then passed dynamically in {@link #readFields(ITuple, Schema, Deserializer[])}. Note that this is
	 * not the most intuitive way of using this class, but it is made for efficiency.
	 */
	SimpleTupleDeserializer(HadoopSerialization ser, Configuration conf) {
		this.ser = ser;
		this.conf = conf;
	}

	/**
	 * Constructor with one Schema. This Schema will be used to read Tuples.
	 */
	public SimpleTupleDeserializer(Schema schemaToDeserialize, HadoopSerialization ser, Configuration conf) {
		this(schemaToDeserialize, schemaToDeserialize, ser, conf);
	}

	/**
	 * Constructor with two schemas. In this case we deserialize one Schema but we write the final result into another one
	 * (which should be backwards compatible).
	 */
	public SimpleTupleDeserializer(Schema readSchema, Schema targetSchema, HadoopSerialization ser,
	    Configuration conf) {
		this(ser, conf);
		this.readSchema = readSchema;
		this.targetSchema = targetSchema;
		// calculate a lookup table for backwards compatibility
		// "UNUSED" will mean the field is not used anymore
		backwardsCompatibiltyLookupVector = new int[readSchema.getFields().size()];
		for(int i = 0; i < readSchema.getFields().size(); i++) {
			backwardsCompatibiltyLookupVector[i] = UNUSED;
			Field field = readSchema.getFields().get(i);
			if(targetSchema.containsField(field.getName())) {
				backwardsCompatibiltyLookupVector[i] = targetSchema.getFieldPos(field.getName());
			}
		}

		deserializers = SerializationInfo.getDeserializers(readSchema, targetSchema, conf);
	}

	@Override
	public void close() throws IOException {
		input.close();
	}

	@Override
	public ITuple deserialize(ITuple tuple) throws IOException {
		if(tuple == null) {
			tuple = new Tuple(targetSchema);
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

	/**
	 * Read fields using the specified "readSchema" in the constructor.
	 */
	public void readFields(ITuple tuple, Deserializer[] customDeserializers) throws IOException {
		readFields(tuple, readSchema, customDeserializers);
	}

	/**
	 * If the deserializer has been configured to use two schemas (read and target) then there is a lookup vector, otherwise
	 * same schema is assumed.
	 */
	private int backwardsCompatibleIndex(int i) {
		return backwardsCompatibiltyLookupVector == null ? i : backwardsCompatibiltyLookupVector[i];
	}

	// Private tuple that will be used to skip certain fields for backwards-compatibility
	private ITuple cachedReadTuple = null;

	/**
	 * Private tuple that will be used to skip certain fields for backwards-compatibility. Because we save cached Objects
	 * in the Tuple for deserializing custom Objects it is convenient to have such a Tuple event if no-one uses it
	 * afterwards.
	 */
	private ITuple cachedReadTuple() {
		if(cachedReadTuple == null) {
			cachedReadTuple = new Tuple(readSchema);
		}
		return cachedReadTuple;
	}

	/**
	 * Read fields using an ad-hoc Schema passed by parameter. This method is package-visibility since this is not the
	 * standard way of using this Deserializer. This method is used by {@link TupleDeserializer}.
	 */
	void readFields(ITuple tuple, Schema schema, Deserializer[] customDeserializers) throws IOException {
		// If there are fields with nulls, read the bit field and set the values that are null
		if(schema.containsNullableFields()) {
			List<Integer> nullableFields = schema.getNullableFieldsIdx();
			nullsAbsolute.ensureSize(schema.getFields().size());
			nullsAbsolute.clear(nullableFields);
			nullsRelative.deser(input);
			for(int i = 0; i < nullableFields.size(); i++) {
				if(nullsRelative.isSet(i)) {
					int field = backwardsCompatibleIndex(nullableFields.get(i));
					if(field != UNUSED) {
						tuple.set(field, null);
					}
					nullsAbsolute.flags[nullableFields.get(i)] = true;
				}
			}
		}

		// Field by field deserialization
		for(int index = 0; index < schema.getFields().size(); index++) {
			Deserializer customDeser = customDeserializers[index];
			Field field = schema.getField(index);

			// Nulls control
			if(field.isNullable() && nullsAbsolute.flags[index]) {
				// Null field. Nothing to deserialize.
				// TODO Add default value if specified
				continue;
			}

			/*
			 * If we configured the Deserializer to use two Schemas,
			 * this will give us the real index for the destination Tuple.
			 * If it gives "UNUSED" it means the field being read is not used.
			 * We will deal with this depending on wether we read a primitive field or 
			 * a complex data type.
			 */
			int idx = backwardsCompatibleIndex(index);

			switch(field.getType()) {
			case INT:
				int iVal = WritableUtils.readVInt(input);
				if(idx != UNUSED) {
					tuple.set(idx, iVal);
				} // If the primitive field is not used we just don't set it
				break;
			case LONG:
				long lVal = WritableUtils.readVLong(input);
				if(idx != UNUSED) {
					tuple.set(idx, lVal);
				} // If the primitive field is not used we just don't set it
				break;
			case DOUBLE:
				double dVal = input.readDouble();
				if(idx != UNUSED) {
					tuple.set(idx, dVal);
				} // If the primitive field is not used we just don't set it
				break;
			case FLOAT:
				float fVal = input.readFloat();
				if(idx != UNUSED) {
					tuple.set(idx, fVal);
				} // If the primitive field is not used we just don't set it
				break;
			case STRING:
				if(idx == UNUSED) {
					// The field is unused so we use a private cached Tuple for skipping its bytes
					readUtf8(input, cachedReadTuple(), index);
				} else {
					readUtf8(input, tuple, idx);
				}
				break;
			case BOOLEAN:
				byte b = input.readByte();
				if(idx != UNUSED) {
					tuple.set(idx, (b != 0));
				} // If the primitive field is not used we just don't set it
				break;
			case ENUM:
				if(idx == UNUSED) {
					// The field is unused so we use a private cached Tuple for skipping its bytes
					readEnum(input, cachedReadTuple(), field.getObjectClass(), index);
				} else {
					readEnum(input, tuple, field.getObjectClass(), idx);
				}
				break;
			case BYTES:
				if(idx == UNUSED) {
					// The field is unused so we use a private cached Tuple for skipping its bytes
					readBytes(input, cachedReadTuple(), index);
				} else {
					readBytes(input, tuple, idx);
				}
				break;
			case OBJECT:
				if(idx == UNUSED) {
					// The field is unused so we use a private cached Tuple for skipping its bytes
					readCustomObject(input, cachedReadTuple(), field.getObjectClass(), index, customDeser);
				} else {
					readCustomObject(input, tuple, field.getObjectClass(), idx, customDeser);
				}
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
			throw new IOException("Error deserializing, custom object serialized with negative length : "
			    + size);
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

	protected void readEnum(DataInputStream input, ITuple tuple, Class<?> fieldType, int index)
	    throws IOException {
		int ordinal = WritableUtils.readVInt(input);
		try {
			Object[] enums = fieldType.getEnumConstants();
			tuple.set(index, enums[ordinal]);
		} catch(ArrayIndexOutOfBoundsException e) {
			throw new IOException("Ordinal index out of bounds for " + fieldType + " ordinal=" + ordinal);
		}
	}

	/**
	 * Helping class that keeps an array of flags. Used to know if a particular field is null or not.
	 */
	private static class FlagsField {
		public boolean[] flags = new boolean[0];

		public void ensureSize(int size) {
			if(flags.length < size) {
				flags = Arrays.copyOf(flags, size);
			}
		}

		public void clear(List<Integer> flags) {
			for(Integer flag : flags) {
				this.flags[flag] = false;
			}
		}
	}
}

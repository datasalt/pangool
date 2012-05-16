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

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Serializer;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.io.Utf8;
import com.datasalt.pangool.serialization.HadoopSerialization;
import com.datasalt.pangool.tuplemr.SerializationInfo;

/**
 * This Serializer holds all the baseline code for serializing Tuples. It is used by the more complex {@link TupleSerializer}.
 * It is also used by a stateful Tuple field serializer {@link TupleFieldSerialization}. 
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class SimpleTupleSerializer implements Serializer<ITuple> {

	private DataOutputStream out;
	private final HadoopSerialization ser;
	private final Utf8 HELPER_TEXT = new Utf8();
	private final DataOutputBuffer tmpOutputBuffer = new DataOutputBuffer();

  private Serializer[] customSerializers;

	// A SimpleTupleSerializer that doesn't serialize a specified Schema
  SimpleTupleSerializer(HadoopSerialization ser) {
		this.ser = ser;
	}
	
	// A SimpelTupleSerializer that serializes a certain Schema
	SimpleTupleSerializer(Schema schemaToSerialize, HadoopSerialization ser, Configuration conf) {
		this(ser);
		this.customSerializers = SerializationInfo.getSerializers(schemaToSerialize, conf);
	}

	@Override
	public void close() throws IOException {
		out.close();
	}

	@Override
	public void open(OutputStream outputStream) {
		if(outputStream instanceof DataOutputStream) {
			out = (DataOutputStream) outputStream;
		} else {
			out = new DataOutputStream(outputStream);
		}
	}

	@Override
	public void serialize(ITuple tuple) throws IOException {
		write(tuple.getSchema(), tuple, null, customSerializers);
	}

	public DataOutputStream getOut() {
		return out;
	}

	public void write(Schema destinationSchema, ITuple tuple, int[] translationTable, Serializer[] customSerializers)
	    throws IOException {
		for(int i = 0; i < destinationSchema.getFields().size(); i++) {
			Field field = destinationSchema.getField(i);
			Type fieldType = field.getType();
			Object element;
			if(translationTable == null) {
				element = tuple.get(i);
			} else {
			  element = tuple.get(translationTable[i]);
			}
			try {
				switch(fieldType) {
				case INT:
					WritableUtils.writeVInt(out, (Integer) element);
					break;
				case LONG:
					WritableUtils.writeVLong(out, (Long) element);
					break;
				case DOUBLE:
					out.writeDouble((Double) element);
					break;
				case FLOAT:
					out.writeFloat((Float) element);
					break;
				case STRING:
					if(element instanceof Text) {
						((Text) element).write(out);
					} else if(element instanceof String) {
						HELPER_TEXT.set((String) element);
						HELPER_TEXT.write(out);
					} else {
						raiseClassCastException(null, field, element);
					}
					break;
				case BOOLEAN:
					out.write((Boolean) element ? 1 : 0);
					break;
				case ENUM:
					writeEnum((Enum<?>) element, field, out);
					break;
				case OBJECT:
					writeCustomObject(element, out, customSerializers[i]);
					break;
				case BYTES:
					writeBytes(element, out);
					break;
				default:
					throw new IOException("Not supported type:" + fieldType);
				}
				// TODO this shouldn't be here because customSerializer can throw these exceptions
			} catch(ClassCastException e) {
				raiseClassCastException(e, field, element);
			} catch(NullPointerException e) {
				raiseNullInstanceException(e, field, element);
			}
		} // End for
	}

	private void writeCustomObject(Object element, DataOutput output, Serializer customSer) throws IOException {
		tmpOutputBuffer.reset();
		if(customSer != null) {
			customSer.open(tmpOutputBuffer);
			customSer.serialize(element);
			customSer.close();
		} else {
			// If no custom serializer defined then use Hadoop Serialization by default
			ser.ser(element, tmpOutputBuffer);
		}
		WritableUtils.writeVInt(output, tmpOutputBuffer.getLength());
		output.write(tmpOutputBuffer.getData(), 0, tmpOutputBuffer.getLength());
	}

	private void writeBytes(Object bytes, DataOutput output) throws IOException {
		if(bytes instanceof byte[]) {
			WritableUtils.writeVInt(output, ((byte[]) bytes).length);
			output.write((byte[]) bytes);
		} else if(bytes instanceof ByteBuffer) {
			ByteBuffer buffer = (ByteBuffer) bytes;
			int pos = buffer.position();
			int start = buffer.arrayOffset() + pos;
			int len = buffer.limit() - pos;
			WritableUtils.writeVInt(output, len);
			output.write(buffer.array(), start, len);
		} else {
			throw new IOException("Not allowed " + bytes.getClass() + " for type " + Type.BYTES);
		}

	}

	private void writeEnum(Enum<?> element, Field field, DataOutput output) throws IOException {
		Enum<?> e = (Enum<?>) element;
		Class<?> expectedType = field.getObjectClass();
		if(e.getClass() != expectedType) {
			throw new IOException("Field '" + field.getName() + "' contains '" + element + "' which is "
			    + element.getClass().getName() + ".The expected type is " + expectedType.getName());
		}
		WritableUtils.writeVInt(output, e.ordinal());
	}

	private void raiseClassCastException(ClassCastException cause, Field field, Object element) throws IOException {
		throw new IOException("Field '" + field.getName() + "' with type: '" + field.getType() + "' can't contain '"
		    + element + "' which is " + element.getClass().getName(), cause);
	}

	private void raiseNullInstanceException(NullPointerException cause, Field field, Object element) throws IOException {
		throw new IOException("Field '" + field.getName() + "' with type " + field.getType() + " can't contain null value",
		    cause);
	}
}

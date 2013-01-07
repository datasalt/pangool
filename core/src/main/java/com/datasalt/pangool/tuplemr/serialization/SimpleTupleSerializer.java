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

import com.datasalt.pangool.io.BitField;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.io.Utf8;
import com.datasalt.pangool.serialization.HadoopSerialization;
import com.datasalt.pangool.tuplemr.SerializationInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Serializer;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * This Serializer holds all the baseline code for serializing Tuples. It is used by the more complex {@link TupleSerializer}.
 * It is also used by a stateful Tuple field serializer {@link TupleFieldSerialization}.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class SimpleTupleSerializer implements Serializer<ITuple> {

  private DataOutputStream out;
  private final HadoopSerialization ser;
  private final Utf8 HELPER_TEXT = new Utf8();
  private final BitField nulls = new BitField();
  private final DataOutputBuffer tmpOutputBuffer = new DataOutputBuffer();

  private Serializer[] customSerializers;
  // Optional in some cases
  private Schema schema = null;

  // A SimpleTupleSerializer that doesn't serialize a specified Schema
  public SimpleTupleSerializer(HadoopSerialization ser) {
    this.ser = ser;
  }

  // A SimpelTupleSerializer that serializes a certain Schema. Needed
  // when using custom stateful serialization.
  public SimpleTupleSerializer(Schema schemaToSerialize, HadoopSerialization ser, Configuration conf) {
    this(ser);
    this.schema = schemaToSerialize;
    this.customSerializers = SerializationInfo.getSerializers(schemaToSerialize, conf);
  }

  @Override
  public void close() throws IOException {
    out.close();
  }

  @Override
  public void open(OutputStream outputStream) {
    if (outputStream instanceof DataOutputStream) {
      out = (DataOutputStream) outputStream;
    } else {
      out = new DataOutputStream(outputStream);
    }
  }

  @Override
  public void serialize(ITuple tuple) throws IOException {
    Schema schema = (this.schema != null) ? this.schema : tuple.getSchema();
    write(schema, tuple, null, customSerializers);
  }

  public DataOutputStream getOut() {
    return out;
  }

  /**
   * @return A value in the tuple represented by the idx. If a translationTable is given,
   *         then idx is translated before being applied to obtain the value from the tuple.
   */
  protected Object valueAt(int idx, ITuple tuple, int[] translationTable) {
    if (translationTable == null) {
      return tuple.get(idx);
    } else {
      return tuple.get(translationTable[idx]);
    }
  }

  void write(Schema destinationSchema, ITuple tuple, int[] translationTable, Serializer[] customSerializers)
      throws IOException {
    // If can be null values, we compose a bit set with the null information and write it the first.
    if (destinationSchema.containsNullableFields()) {
      List<Integer> nullableFields = destinationSchema.getNullableFieldsIdx();
      nulls.clear();
      for (int i = 0; i < nullableFields.size(); i++) {
        int nField = nullableFields.get(i);
        if (valueAt(nField, tuple, translationTable) == null) {
          nulls.set(i);
        }
      }
      nulls.ser(out);
    }

    for (int i = 0; i < destinationSchema.getFields().size(); i++) {
      Field field = destinationSchema.getField(i);
      Type fieldType = field.getType();
      Object element = valueAt(i, tuple, translationTable);
      if (element == null) {
        if (field.isNullable()) {
          // Nullable null fields don't need serialization.
          continue;
        } else {
          raiseUnexpectedNullException(field, element);
        }
      }
      try {
        switch (fieldType) {
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
            if (element instanceof Text) {
              ((Text) element).write(out);
            } else if (element instanceof String) {
              HELPER_TEXT.set((String) element);
              HELPER_TEXT.write(out);
            } else {
              raisedClassCastException(null, field, element);
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
      } catch (ClassCastException e) {
        raisedClassCastException(e, field, element);
      } catch (CustomObjectSerializationException e) {
        raisedCustomObjectException(e, field, element, customSerializers[i]);
      }
    } // End for
  }

  private void writeCustomObject(Object element, DataOutput output, Serializer customSer) throws CustomObjectSerializationException {
    try {
      tmpOutputBuffer.reset();
      if (customSer != null) {
        customSer.open(tmpOutputBuffer);
        customSer.serialize(element);
        customSer.close();
      } else {
        // If no custom serializer defined then use Hadoop Serialization by default
        ser.ser(element, tmpOutputBuffer);
      }
      WritableUtils.writeVInt(output, tmpOutputBuffer.getLength());
      output.write(tmpOutputBuffer.getData(), 0, tmpOutputBuffer.getLength());
    } catch (Throwable e) {
      throw new CustomObjectSerializationException(e);
    }
  }

  private void writeBytes(Object bytes, DataOutput output) throws IOException {
    if (bytes instanceof byte[]) {
      WritableUtils.writeVInt(output, ((byte[]) bytes).length);
      output.write((byte[]) bytes);
    } else if (bytes instanceof ByteBuffer) {
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
    if (e.getClass() != expectedType) {
      throw new IOException("Field '" + field.getName() + "' contains '" + element + "' which is "
          + element.getClass().getName() + ".The expected type is " + expectedType.getName());
    }
    WritableUtils.writeVInt(output, e.ordinal());
  }

  private void raisedClassCastException(ClassCastException cause, Field field, Object element) throws IOException {
    throw new IOException("Field '" + field.getName() + "' with type: '" + field.getType() + "' can't contain '"
        + element + "' which is " + element.getClass().getName(), cause);
  }

  private void raiseUnexpectedNullException(Field field, Object element) throws IOException {
    throw new IOException("Field '" + field.getName() + "' with type " + field.getType() +
        " can't contain null value");
  }

  private void raisedCustomObjectException(CustomObjectSerializationException cause, Field field, Object element, Serializer serializer) throws IOException {
    throw new IOException("Custom object field '" + field.getName() + " with value " + element +
        " of type " + ((element != null) ? element.getClass().getCanonicalName() : "null") +
        " using serializer " + serializer + " thrown an exception.",
        cause);
  }

  /**
   * Thrown when an unexpected exception happens when serializing a custom object.
   */
  public static class CustomObjectSerializationException extends Exception {
    public CustomObjectSerializationException() {
    }

    public CustomObjectSerializationException(String message) {
      super(message);
    }

    public CustomObjectSerializationException(String message, Throwable cause) {
      super(message, cause);
    }

    public CustomObjectSerializationException(Throwable cause) {
      super(cause);
    }
  }
}

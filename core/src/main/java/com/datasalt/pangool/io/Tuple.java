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
package com.datasalt.pangool.io;

import com.datasalt.pangool.PangoolRuntimeException;
import com.datasalt.pangool.io.Schema.Field;
import org.apache.hadoop.io.Text;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

/**
 * This is the basic implementation of {@link ITuple}.
 */
@SuppressWarnings("serial")
public class Tuple implements ITuple, Serializable, Cloneable {

  private Object[] array;
  private Schema schema;

  public Tuple(Schema schema) {
    this.schema = schema;
    int size = schema.getFields().size();
    this.array = new Object[size];
  }

  @Override
  public Object get(int pos) {
    return array[pos];
  }

  @Override
  public void set(int pos, Object object) {
    array[pos] = object;
  }

  @Override
  public String toString() {
    return toString(this);
  }

  public static String toString(ITuple tuple) {
    Schema schema = tuple.getSchema();
    StringBuilder b = new StringBuilder();
    b.append("{");
    for (int i = 0; i < schema.getFields().size(); i++) {
      Field f = schema.getField(i);
      if (i != 0) {
        b.append(",");
      }
      b.append("\"").append(f.getName()).append("\"").append(":");
      if (tuple.get(i) == null) {
        b.append("null");
        continue;
      }
      switch (f.getType()) {
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case BOOLEAN:
          b.append(tuple.get(i));
          break;
        case STRING:
        case ENUM:

          b.append("\"").append(tuple.get(i)).append("\"");
          break;
        case OBJECT:
          b.append("{").append(tuple.get(i).toString()).append("}");
          break;
        case BYTES:
          Object o = tuple.get(i);
          b.append("{\"bytes\": \"");
          byte[] bytes;
          int offset, length;
          if (o instanceof ByteBuffer) {
            ByteBuffer byteBuffer = (ByteBuffer) o;
            bytes = byteBuffer.array();
            offset = byteBuffer.arrayOffset() + byteBuffer.position();
            length = byteBuffer.limit() - byteBuffer.position();
          } else {
            //byte[]
            bytes = (byte[]) o;
            offset = 0;
            length = bytes.length;
          }
          for (int p = offset; p < offset + length; p++) {
            b.append((char) bytes[p]);
          }
          b.append("\"}");
          break;
        default:
          throw new PangoolRuntimeException("Not stringifiable type :" + f.getType());
      }
    }
    b.append("}");
    return b.toString();
  }

  @Override
  public void clear() {
    for (int i = 0; i < array.length; i++) {
      array[i] = null;
    }
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public Object get(String field) {
    Integer pos = schema.getFieldPos(field);
    if (pos == null) {
      throw new IllegalArgumentException("Field '" + field + "' not present in schema " + schema);
    }
    return get(pos);
  }

  @Override
  public void set(String field, Object object) {
    Integer pos = schema.getFieldPos(field);
    if (pos == null) {
      throw new IllegalArgumentException("Field '" + field + "' not present in schema " + schema);
    }
    set(pos, object);
  }

  public boolean equals(Object that) {
    if (!(that instanceof ITuple)) {
      return false;
    }
    boolean schemaEquals = this.schema.equals(((ITuple) that).getSchema());
    if (!schemaEquals) {
      return false;
    }

    for (int i = 0; i < array.length; i++) {
      Object o1 = get(i);
      Object o2 = ((ITuple) that).get(i);
      if (o1 == null) {
        if (o2 != null) {
          return false;
        }
      } else {
        // TODO this special case shouldn't be treated here.Tuples don't care
        // about Texts or Strings.
        // Create a new equals method outside that takes in consideration
        // this particular case concerning Serialization/deser
        if (o1 instanceof Text) {
          o1 = o1.toString();
        }
        if (o2 != null && o2 instanceof Text) {
          o2 = o2.toString();
        }
        if (!o1.equals(o2)) {
          return false;
        }
      }
    }
    return true;
  }

  public int hashCode() {
    assert false : "hashCode not designed";
    return 42; // any arbitrary constant will do
  }

	
  /**
   * Simple shallow copy of this Tuple to another Tuple.
	 *
   * @param tupleDest The destination Tuple (should have the same Schema or a super-set of it).
   */
	public void shallowCopy(ITuple tupleDest) {
		for(Field field: this.getSchema().getFields()) {
			tupleDest.set(field.getName(), this.get(field.getName()));
		}
	}
	
  /**
   * Performs a deep copy of this Tuple. <br/>
   * Custom {@link FieldClonator}s can be provided for each individual field.
   *
   * @return a copy of the tuple
   * @throws IllegalArgumentException    if the value of a particular field does not match with the type
   *                                     on the schema for this field.
   * @throws IDontKnowHowToCopyThisStuff for fields of type {@link Field.Type#OBJECT}. See
   *                                     {@link #deepCopy(ITuple, java.util.Map)} for solving this issue with {@link FieldClonator}
   */
  public Tuple deepCopy() {
    return deepCopy(this);
  }

  /**
   * Performs a deep copy of this Tuple. <br/>
   * Custom {@link FieldClonator}s can be provided for each individual field.
   *
   * @param customClonators map with custom {@link FieldClonator} for each field, indexed by field name.
   * @return a copy of the tuple
   * @throws IllegalArgumentException    if the value of a particular field does not match with the type
   *                                     on the schema for this field.
   * @throws IDontKnowHowToCopyThisStuff for fields of type {@link Field.Type#OBJECT} without a custom
   *                                     {@link FieldClonator}. Provide always corresponding {@link FieldClonator}s for this type of fields.
   */
  public Tuple deepCopy(Map<String, FieldClonator> customClonators) {
    return deepCopy(this, customClonators);
  }

  /**
   * Performs a deep copy of the given Tuple. <br/>
   * Custom {@link FieldClonator}s can be provided for each individual field.
   *
   * @param tuple a tuple to copy.
   * @return a copy of the tuple
   * @throws IllegalArgumentException    if the value of a particular field does not match with the type
   *                                     on the schema for this field.
   * @throws IDontKnowHowToCopyThisStuff for fields of type {@link Field.Type#OBJECT}. See
   *                                     {@link #deepCopy(ITuple, java.util.Map)} for solving this issue with {@link FieldClonator}
   */
  public static Tuple deepCopy(ITuple tuple) {
    return deepCopy(tuple, null);
  }

  /**
   * Performs a deep copy of the given Tuple. <br/>
   * Custom {@link FieldClonator}s can be provided for each individual field.
   *
   * @param tuple           a tuple to copy.
   * @param customClonators map with custom {@link FieldClonator} for each field, indexed by field name.
   * @return a copy of the tuple
   * @throws IllegalArgumentException    if the value of a particular field does not match with the type
   *                                     on the schema for this field.
   * @throws IDontKnowHowToCopyThisStuff for fields of type {@link Field.Type#OBJECT} without a custom
   *                                     {@link FieldClonator}. Provide always corresponding {@link FieldClonator}s for this type of fields.
   */
  public static Tuple deepCopy(ITuple tuple, Map<String, FieldClonator> customClonators) {
    Schema schema = tuple.getSchema();
    Tuple newTuple = new Tuple(schema);
    for (int i = 0; i < schema.getFields().size(); i++) {
      Field field = schema.getField(i);

      Object value = tuple.get(i);
      if (value == null) {
        continue;
      }

      if (customClonators != null && customClonators.containsKey(field.getName())) {
        // There is a custom clonator
        newTuple.set(i, customClonators.get(field.getName()).giveMeACopy(value));
        continue;
      }

      switch (field.getType()) {
        case BYTES:
          if (value instanceof ByteBuffer) {
            newTuple.set(i, cloneByteBuffer((ByteBuffer) value));
          } else if (value instanceof byte[]) {
            newTuple.set(i, Arrays.copyOf((byte[]) value, ((byte[]) value).length));
          } else {
            throw new IllegalArgumentException("Field " + field.getName() + " of type " + field.getType()
                + " cannot contains values of class " + value.getClass().getCanonicalName());
          }
          break;
        case OBJECT:
          if (value instanceof ITuple) {
            throw new IDontKnowHowToCopyThisStuff("Tuples inside tuples requires a custom FieldClonator" +
                "to perform the copy. Please, provide a custom FieldClonator for field " + field.getName()
                + ". It usually is as simple as create one that calls the deepCopy method for the " +
                "inner tuple");
          } else {
            throw new IDontKnowHowToCopyThisStuff("I don't know how to copy the field " + field.getName()
                + " with type " + value.getClass().getCanonicalName() + ". Please, provide a custom " +
                "FieldClonator for this field in order to be able to perform deep copies");
          }
        case STRING:
          if (value instanceof String) {
            newTuple.set(i, tuple.get(i));
          } else if (value instanceof Utf8 || value instanceof Text) {
            newTuple.set(i, new Utf8(value.toString()));
          } else {
            throw new IllegalArgumentException("Field " + field.getName() + " of type " + field.getType()
                + " cannot contains values of class " + value.getClass().getCanonicalName());
          }
          break;
        default:
          newTuple.set(i, tuple.get(i));
      }
    }
    return newTuple;
  }

  /**
   * Thrown by {@link #deepCopy(ITuple, java.util.Map)} in the case of field
   * of a type that Pangool doesn't know how to copy it. In this case,
   * a solution should be to provide a proper {@link FieldClonator}
   * for the particular field.
   */
  public static class IDontKnowHowToCopyThisStuff extends RuntimeException {
    public IDontKnowHowToCopyThisStuff(String message) {
    }
  }

  private static ByteBuffer cloneByteBuffer(ByteBuffer original) {
    ByteBuffer clone = ByteBuffer.allocate(original.capacity());
    original.rewind();//copy from the beginning
    clone.put(original);
    original.rewind();
    clone.flip();
    return clone;
  }
}

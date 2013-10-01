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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.io.IntWritable;
import org.apache.thrift.TBase;
import org.junit.Test;

import com.datasalt.pangool.BaseTest;

public class TestTuple extends BaseTest {

  @Test
  public void testDeepCopy() throws Exception {
    Schema schema = new Schema("schema", Fields.parse("str1:string,str2:string,b1:bytes,b2:bytes,tuple:com.datasalt.pangool.io.Tuple,object:org.apache.hadoop.io.IntWritable,int:int,null:org.apache.hadoop.io.IntWritable"));
    Tuple t = new Tuple(schema);
    t.set("str1", "hola");
    t.set("str2", new Utf8("hola"));
    byte[] data = new byte[]{(byte) 0xab, (byte) 0x12};
    ByteBuffer bf = ByteBuffer.wrap(data);
    t.set("b1", data);
    t.set("b2", bf);
    Schema schema2 = new Schema("schema2", Fields.parse("str:string"));
    Tuple innerT = new Tuple(schema2);
    innerT.set(0, "pepe");
    t.set("int", 4);
    t.set("null", null);

    // IDontKnow assertions
    t.set("tuple", innerT);
    assertIDontKnow(t);
    t.set("tuple", null);

    t.set("object", new IntWritable(3));
    assertIDontKnow(t);
    t.set("object", null);

    // Assert clone
    t.set("tuple", innerT);
    t.set("object", new IntWritable(3));

    HashMap<String, FieldClonator> clonators = new HashMap<String, FieldClonator>();
    clonators.put("tuple", new FieldClonator() {
      @Override
      public Object giveMeACopy(Object value) {
        return ((Tuple) value).deepCopy();
      }
    });
    clonators.put("object", new FieldClonator() {
      @Override
      public Object giveMeACopy(Object value) {
        return new IntWritable(((IntWritable) value).get());
      }
    });

    assertIsAClone(t, t.deepCopy(clonators));
  }

  public static void assertIsAClone(Tuple t1, Tuple t2) {
    Schema schema = t1.getSchema();
    assertEquals(schema, t2.getSchema());

    String msg = t1 + " vs " + t2;

    for (int i = 0; i < schema.getFields().size(); i++) {
      Schema.Field field = schema.getField(i);
      msg = "Field: " + field.getName() + " - " + msg;
      Object value1 = t1.get(i);
      Object value2 = t2.get(i);
      if (value1 == null) {
        assertNull(msg, value2);
      } else if (value1 instanceof byte[]) {
        assertTrue(msg, Arrays.equals((byte[]) value1, (byte[]) value2));
      } else {
        assertEquals(msg, value1, value2);
      }
      // not same instance
      if (field.getType() == Schema.Field.Type.OBJECT || field.getType() == Schema.Field.Type.BYTES) {
        assertTrue(msg, t1.get(i) != t2.get(i) || (t1.get(i) == null && t2.get(i) == null));
      }
    }
  }

  public static void assertIDontKnow(Tuple tuple) {
    boolean exception = false;
    try {
      tuple.deepCopy(tuple);
    } catch (Tuple.IDontKnowHowToCopyThisStuff e) {
      exception = true;
    }
    assertTrue(tuple + "", exception);
  }

  @Test
  public void testDeepCopy2() throws Exception {
    int NUM_TUPLES = 200;

    HashMap<String, FieldClonator> clonators = new HashMap<String, FieldClonator>();
    clonators.put("thrift_field", new FieldClonator() {
      @Override
      public Object giveMeACopy(Object value) {
        return ((TBase) value).deepCopy();
      }
    });
    clonators.put("my_avro", new FieldClonator() {
      @Override
      public Object giveMeACopy(Object value) {
        GenericData.Record r = (GenericData.Record) value;
        GenericData.Record copy = new GenericData.Record(r, true);
        return copy;
      }
    });

    Tuple t = new Tuple(decorateWithNullables(SCHEMA));
    for (int i = 0; i < NUM_TUPLES; i++) {
      fillTuple(true, t);
      assertIsAClone(t, t.deepCopy(clonators));
    }
  }

  @Test
  public void testNewGetters() {
  	Schema schema = new Schema("foo", Fields.parse("a:int, b:double, c:long, d:string, e:float, f:boolean"));
  	Tuple tuple = new Tuple(schema);
  	tuple.set(0, 10);
  	tuple.set("b", 4.5d);
  	tuple.set(2, 1000l);
  	tuple.set("d", "foo1");
  	tuple.set(4, 10.5f);
  	tuple.set(5, true);
  	
  	assertEquals(tuple.get(0), 10);
  	assertEquals(tuple.get(1), 4.5d);
  	assertEquals(tuple.get(2), 1000l);
  	assertEquals(tuple.get(3).toString(), "foo1");
  	assertEquals(tuple.get(4), 10.5f);
  	assertEquals(tuple.get(5), true);
  	
		assertEquals(tuple.getInteger(0), tuple.get(0));
		assertEquals(tuple.getDouble(1), tuple.get(1));
		assertEquals(tuple.getLong(2), tuple.get(2));
		assertEquals(tuple.getString(3), tuple.get(3).toString());
		assertEquals(tuple.getFloat(4), tuple.get(4));
		assertEquals(tuple.getBoolean(5), tuple.get(5));
		
		assertEquals(tuple.getInteger("a"), tuple.get(0));
		assertEquals(tuple.getDouble("b"), tuple.get(1));
		assertEquals(tuple.getLong("c"), tuple.get(2));
		assertEquals(tuple.getString("d"), tuple.get(3).toString());
		assertEquals(tuple.getFloat("e"), tuple.get(4));
		assertEquals(tuple.getBoolean("f"), tuple.get(5));
  }
}

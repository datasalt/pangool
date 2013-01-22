package com.datasalt.pangool.io;

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

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;

import static com.datasalt.pangool.io.BitField.byteForBit;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link com.datasalt.pangool.io.BitField}
 */
public class TestBitField {

  @Test
  public void testSet() throws Exception {
    int MAX = 200;
    int BITS = 100;
    Random r = new Random(0);
    BitField bf = new BitField();
    HashSet<Integer> bits = new HashSet<Integer>();
    for(int i=0;i<BITS; i++) {
      int bit = r.nextInt(MAX);
      bf.set(bit);
      bits.add(bit);
    }
    // Asserting sets
    for(int bit : bits) {
      assertTrue(bf.isSet(bit));
    }
    // Asserting unsets
    for(int i = 0; i<MAX; i++) {
      if (!bits.contains(i)) {
        assertFalse(bf.isSet(i));
      }
    }
  }

  @Test
  public void testUnset() throws Exception {
    int MAX = 200;
    int BITS = 100;
    Random r = new Random(0);
    BitField bf = new BitField();

    // Setting all bits
    for(int i = 0; i<MAX; i++) {
      bf.set(i);
    }

    HashSet<Integer> bits = new HashSet<Integer>();
    for(int i=0;i<BITS; i++) {
      int bit = r.nextInt(MAX);
      bf.unset(bit);
      bits.add(bit);
    }
    // Asserting unsets
    for(int bit : bits) {
      assertFalse(bf.isSet(bit));
    }
    // Asserting sets
    for(int i = 0; i<MAX; i++) {
      if (!bits.contains(i)) {
        assertTrue(bf.isSet(i));
      }
    }
  }

  @Test
  public void testCornerCases() throws Exception {
    BitField bf = new BitField();
    assertFalse(bf.isSet(24));
    assertFalse(bf.isSet(0));
  }

  public static BitField from(String bits) {
    BitField bf = new BitField();
    int bit = 0;
    for (int i = bits.length()-1; i>=0; i++) {
      if (bits.charAt(i) == '1' || bits.charAt(i) == '0') {
        bf.set(bit++, (bits.charAt(i) == '1'));
      }
    }
    return bf;
  }

  @Test
  public void testCompareTo() {
    BitField b1 = new BitField(), b2 = new BitField();
    assertTrue(b1.compareTo(b2) == 0);
    b1.set(0);
    assertTrue(b1.compareTo(b2) > 0);
    b2.set(0);
    assertTrue(b1.compareTo(b2) == 0);
    b2.set(1);
    assertTrue(b1.compareTo(b2) < 0);
    b2.unset(1);
    b2.set(100);
    b2.unset(100);
    assertTrue(b1.compareTo(b2) == 0);
    b1.set(8);
    b2.set(8);
    assertTrue(b1.compareTo(b2) == 0);

    for(int i=0; i<100; i++) {
      b1.set(i);
      b2.set(i);
    }
    assertTrue(b1.compareTo(b2) == 0);
    b1.set(1000);
    assertTrue(b1.compareTo(b2) > 0);
    b2.set(1000);
    assertTrue(b1.compareTo(b2) == 0);
    b2.set(1001);
    assertTrue(b1.compareTo(b2) < 0);
  }

  static void serDeCheck(BitField bf) throws IOException {
    DataOutputBuffer dob = new DataOutputBuffer();
    DataInputBuffer dib = new DataInputBuffer();
    bf.ser(dob);
    dib.reset(dob.getData(), dob.getData().length);
    // Checking DataInput deser
    BitField newbf = new BitField();
    newbf.set(1000);
    newbf.deser(dib);
    assertEquals(bf.toString() + " vs " + newbf.toString(), 0, bf.compareTo(newbf));
    // Asserting that the rightmost bit is always 0. Important for comparisons
    byte [] arr = newbf.getBackingArray();
    for (int i = 0; i<arr.length; i++) {
      assertEquals(0, arr[i] & 1);
    }

    //Checking byte array deserialization
    newbf = new BitField();
    newbf.set(1000);
    newbf.deser(dob.getData(), 0);
    assertEquals(bf.toString() + " vs " + newbf.toString(), 0, bf.compareTo(newbf));
    // Asserting that the rightmost bit is always 0. Important for comparisons
    arr = newbf.getBackingArray();
    for (int i = 0; i<arr.length; i++) {
      assertEquals(0, arr[i] & 1);
    }
  }

  @Test
  public void testDeSer() throws Exception {
    BitField b1 = new BitField();
    serDeCheck(b1);
    b1.set(0);
    serDeCheck(b1);
    b1.unset(0);
    serDeCheck(b1);
    b1.set(0);
    b1.set(1);
    serDeCheck(b1);
    b1.set(7);
    serDeCheck(b1);
    b1.set(13);
    serDeCheck(b1);
    b1.set(14);
    serDeCheck(b1);

    Random r = new Random(0);
    int MAX_BIT = 100;
    int ITERATIONS = 40;
    for (int i=0; i<ITERATIONS; i++) {
      b1.set(r.nextInt(MAX_BIT));
      serDeCheck(b1);
    }
  }


  static void checkReturnOfDeser(int bits) throws IOException {
    BitField bf = new BitField();
    bf.set(bits - 1 );
    DataOutputBuffer dob = new DataOutputBuffer();
    DataInputBuffer dib = new DataInputBuffer();
    bf.ser(dob);
    dib.reset(dob.getData(), dob.getData().length);
    // Checking DataInput deser
    BitField newbf = new BitField();
    int read = newbf.deser(dib);
    assertEquals("For bits " + bits, ((bits-1)/7)+1, read);

    //Checking byte array deserialization
    newbf = new BitField();
    read = newbf.deser(dob.getData(), 0);
    assertEquals("For bits " + bits, ((bits-1)/7)+1, read);
  }

  @Test
  public void testCheckReturnOfDeser() throws IOException {
    for (int i = 0; i < 100; i++) {
      checkReturnOfDeser(i);
    }
  }


  @Test
  public void testByteForBit() throws Exception {
    assertEquals(0, byteForBit(0));
    assertEquals(0, byteForBit(6));
    assertEquals(1, byteForBit(7));
    assertEquals(1, byteForBit(13));
    assertEquals(2, byteForBit(14));
  }

  @Test
  public void testClear() throws Exception {
    BitField b1 = new BitField();
    assertEmpty(b1);
    b1.set(1);
    b1.clear();
    assertEmpty(b1);
    Random r = new Random(0);
    int MAX_BIT = 100;
    int ITERATIONS = 40;
    for (int i=0; i<ITERATIONS; i++) {
      b1.set(r.nextInt(MAX_BIT));
    }
    b1.clear();
    assertEmpty(b1);
  }

  public void assertEmpty(BitField bf) {
    for (int i=0; i<bf.getBackingArray().length*7; i++) {
      assertFalse(bf.toString(), bf.isSet(i));
    }
  }


    @Test
  public void ensureSpace() throws Exception {
    BitField f = new BitField();
    assertEquals(0, f.getBackingArray().length);
    f.ensureSpace(0);
    assertEquals(0, f.getBackingArray().length);
    f.ensureSpace(1);
    assertEquals(1, f.getBackingArray().length);
    f.ensureSpace(0);
    assertEquals(1, f.getBackingArray().length);
    f.ensureSpace(3);
    assertEquals(3, f.getBackingArray().length);

  }
}

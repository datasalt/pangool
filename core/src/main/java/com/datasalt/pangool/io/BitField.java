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

import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Bit Field class. It can holds unbounded bit fields, that can be compared and serialized.
 * The internal representation uses one bit per byte for internal usage, so this class is able
 * to represent 7 bits of information per each byte.
 * <br/>
 * <strong>Serialization:</strong> BitField uses a variable length serialization. The minimum size
 * is one byte when the bits that are set fits on one byte. That is, when there are not bits
 * set above the 6 position. 2 bytes if there are not bits set above the position 13, and so on.
 * See {@link #ser(java.io.DataOutput)} for more information.
 * <br/>
 * <strong>Comparison:</strong> BitField compares bits lexicographically. Lower position bits are
 * smaller than bigger ones.
 * <br/>
 * <strong>Internal in memory representation:</strong> A byte array contains all the bits. Each byte
 * contains 7 bits on the most significant bits. The less significant bit of each byte is not used.
 * This representation is used to be quite close to the serialized representation, where the less
 * significant big states if there are more bytes to be read.
 */
public class BitField implements Comparable<BitField> {

  private byte[] bits = new byte[0];

  public BitField() {
  }

  /**
   * Sets or unsets a bit. The smaller allowed bit is 0
   */
  public void set(int bit, boolean value) {
    int bite = byteForBit(bit);
    ensureSpace(bite + 1);
    int bitOnByte = bitOnByte(bit, bite);
    if (value) {
      bits[bite] = byteBitSet(bitOnByte, bits[bite]);
    } else {
      bits[bite] = byteBitUnset(bitOnByte, bits[bite]);
    }
  }

  /**
   * Sets a bit. The smaller allowed bit is 0
   */
  public void set(int bit) {
    set(bit, true);
  }

  /**
   * Unsets a bit. The smaller allowed bit is 0
   */
  public void unset(int bit) {
    set(bit, false);
  }

  /**
   * Returns the value of a given bit. False is returned for unexisting bits.
   */
  public boolean isSet(int bit) {
    int bite = byteForBit(bit);
    if (bite >= bits.length || bits.length == 0) {
      return false;
    }
    int bitOnByte = bitOnByte(bit, bite);
    return ((1 << bitOnByte) & bits[bite]) != 0;
  }

  /**
   * Serializes the bit field to the data output. It uses one byte per each 7 bits. If the
   * rightmost bit of the read byte is set, that means that there are more bytes to consume.
   * The latest byte has the rightmost bit unset.
   */
  public void ser(DataOutput out) throws IOException {
    if (bits.length == 0) {
      out.writeByte(0);
      return;
    }
    // removing trailing empty bytes.
    int bytesToWrite;
    for (bytesToWrite = bits.length; bytesToWrite > 1 && bits[bytesToWrite - 1] == 0; bytesToWrite--) ;
    // Writing first bytes, with the rightmost bit set
    for (int i = 0; i < (bytesToWrite - 1); i++) {
      out.writeByte((bits[i] | 1));
    }
    // Writing the last byte, with the rightmost bit unset
    out.writeByte((bits[bytesToWrite - 1] & ~1));
  }

  /**
   * Deserialize a BitField serialized using {@link #ser(java.io.DataOutput)}.
   * Return the number of bytes consumed.
   */
  public int deser(DataInput in) throws IOException {
    int idx = 0;
    byte current;
    do {
      current = in.readByte();
      ensureSpace(idx + 1);
      // The last bit must be clear
      bits[idx] = (byte) (current & ~1);
      idx++;
    } while ((current & 1) != 0);
    // clear the remaining bytes.
    for (int i = idx; i < bits.length; i++) {
      bits[i] = 0;
    }
    return idx;
  }

  /**
   * Deserialize a BitField serialized from a byte array.
   * Return the number of bytes consumed.
   */
  public int deser(byte[] bytes, int start) throws IOException {
    int idx = 0;
    byte current;
    do {
      current = bytes[start+idx];
      ensureSpace(idx + 1);
      // The last bit must be clear
      bits[idx] = (byte) (current & ~1);
      idx++;
    } while ((current & 1) != 0);
    // clear the remaining bytes.
    for (int i = idx; i < bits.length; i++) {
      bits[i] = 0;
    }
    return idx;
  }


  /**
   * Returns the backing array keeping the bits. Be careful because the array is a reference.
   */
  protected byte[] getBackingArray() {
    return bits;
  }

  /**
   * Minimum size of the backing array needed for setting the given bit
   */
  protected static int byteForBit(int bit) {
    return (bit / 7);
  }

  protected static int bitOnByte(int bit, int bite) {
    // +1 because we leave the rightmost bit always unused
    return (bit - (bite * 7)) + 1;
  }

  /**
   * Ensures a minimum size for the backing byte array
   */
  protected void ensureSpace(int bytes) {
    if (bits.length < bytes) {
      bits = Arrays.copyOf(bits, bytes);
    }
  }

  protected static byte byteBitSet(int bit, byte bite) {
    int mask = 1 << bit;
    return (byte) ((int) bite | mask);
  }

  protected static byte byteBitUnset(int bit, byte bite) {
    int mask = ~(1 << bit);
    return (byte) ((int) bite & mask);
  }

  @Override
  public int compareTo(BitField o) {
    // First we compare the common part
    int common = Math.min(bits.length, o.bits.length);
    int cmp = WritableComparator.compareBytes(bits, 0, common, o.bits, 0, common);
    if (cmp != 0) {
      return cmp;
    }
    // Second, we see if the array trailing part of the longest array is zero. In this case, both are
    // equal
    boolean allZeros = true;
    byte[] longestArray = o.bits;
    if (bits.length > common) {
      longestArray = bits;
    }
    for (int i = common; i < longestArray.length; i++) {
      if (longestArray[i] != 0) {
        allZeros = false;
        break;
      }
    }
    if (allZeros) {
      return 0;
    } else {
      return bits.length - o.bits.length;
    }
  }

  /**
   * Clears the bit field, unsetting all bits.
   */
  public void clear() {
    for (int i = 0; i < bits.length; i++) {
      bits[i] = 0;
    }
  }

  /**
   * Simple lexicographic BitField comparator
   */
  public class BitFieldComparator implements Comparator<BitField> {
    @Override
    public int compare(BitField o1, BitField o2) {
      return o1.compareTo(o2);
    }
  }

  /**
   * Returns the hexadecimal representation of the set backing array
   */
  public String toString() {
    if (bits.length == 0) {
      return "";
    }
    BigInteger bi = new BigInteger(1, bits);
    return String.format("%0" + (bits.length << 1) + "X", bi);
  }
}

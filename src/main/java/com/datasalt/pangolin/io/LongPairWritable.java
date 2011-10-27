package com.datasalt.pangolin.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/** A WritableComparable for two longs. */
@SuppressWarnings("rawtypes")
public class LongPairWritable implements WritableComparable, Cloneable {
  private long value1;
  private long value2;

  public LongPairWritable() {}

  public LongPairWritable(long value1, long value2) { setValue1(value1); setValue2(value2); }

  /** Set the value1 of this PairLongWritable. */
  public void setValue1(long value) { this.value1 = value; }
  
  /** Set the value2 of this PairLongWritable. */
  public void setValue2(long value) { this.value2 = value; }

  /** Return the value1 of this PairLongWritable. */
  public long getValue1() { return value1; }
  
  /** Return the value1 of this PairLongWritable. */
  public long getValue2() { return value2; }

  public void readFields(DataInput in) throws IOException {
    value1 = in.readLong();
    value2 = in.readLong();
  }

  public void write(DataOutput out) throws IOException {
    out.writeLong(value1);
    out.writeLong(value2);
  }

  /** Returns true iff <code>o</code> is a PairLongWritable with the same value. */
  public boolean equals(Object o) {
    if (!(o instanceof LongPairWritable))
      return false;
    LongPairWritable other = (LongPairWritable)o;
    boolean equalsFirst = this.value1 == other.value1;
    if (!equalsFirst) {
    	return false;
    } else {
    	return this.value2 == other.value2; 
    }
  }

  public int hashCode() {
    return (int) (value1 | value2);
  }

  /** Compares two PairLongWritables. */
  public int compareTo(Object o) {
  	LongPairWritable other = (LongPairWritable) o;
  	int cmp1 = compareLong(value1, other.value1);
  	if (cmp1 != 0) {
  		return cmp1;
  	} else {
  		return compareLong(value2, other.value2);
  	}
  }
  
  protected static int compareLong(long l1, long l2) {
  	return (l1<l2 ? -1 : (l1==l2 ? 0 : 1));
  }
  
  

  @Override
  public Object clone() throws CloneNotSupportedException {
	  return super.clone();
  }

	public String toString() {
    return "[" + Long.toString(value1)+ "," + Long.toString(value2) + "]";
  }

  /** A Comparator optimized for PairLongWritable. */ 
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(LongPairWritable.class);
    }

    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      long thisValue = readLong(b1, s1);
      long thatValue = readLong(b2, s2);
      int ret = compareLong(thisValue, thatValue);
      if (ret != 0) {
      	return ret;
      }
      thisValue = readLong(b1, s1 + (Long.SIZE/8));
      thatValue = readLong(b2, s2 + (Long.SIZE/8));
      return compareLong(thisValue, thatValue);
    }
  }

  /** A decreasing Comparator optimized for PairLongWritable. */ 
  public static class DecreasingComparator extends Comparator {
    public int compare(WritableComparable a, WritableComparable b) {
      return -super.compare(a, b);
    }
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return -super.compare(b1, s1, l1, b2, s2, l2);
    }
  }

  static {                                       // register default comparator
    WritableComparator.define(LongPairWritable.class, new Comparator());
  }

}


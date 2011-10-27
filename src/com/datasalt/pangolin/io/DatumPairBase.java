package com.datasalt.pangolin.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.thrift.TException;

/**
 * A class with two items, stored as byte arrays. These arrays can be
 * objects serialized with the {@link Serialization}.
 * <br/>
 * The advantages of this pair is that the comparison is done at the binary level,
 * without deserializing the items. That can be useful for using this class as key 
 * on a Map Reduce job.
 * <br/>
 * Both items must be present and cannot be null.
 * <p>
 * This class have the getters and setters as protected. To use
 * this class, override and create your own setters (recommended
 * for the understanding of the code), or use the class {@link DatumPair} 
 *
 * @author ivan
 *
 */
@SuppressWarnings("rawtypes")
public class DatumPairBase implements WritableComparable {

  private BytesWritable item1= new BytesWritable();
  private BytesWritable item2 = new BytesWritable();
  
  // Default constructor needed.
  public DatumPairBase() {
    
  }
  
  public DatumPairBase(byte [] item1, byte[] item2) throws TException {
    setItem1(item1);
    setItem2(item2);
  }
    
  /**
   * Raw datum getter
   */
  protected BytesWritable getItem1() {
  	return item1;
  }
  
  /**
   * Raw datum getter
   */
  protected BytesWritable getItem2() {
  	return item2;
  }
    
  /**
   * Raw datum setting. 
   */
  protected void setItem1(byte[] datum) {
    item1.set(datum,0,datum.length);
  }
  
  protected void setItem1(byte[] datum,int offset,int length) {
    item1.set(datum,offset,length);
  }
  
  protected void setItem1(BytesWritable datum) {
    item1.set(datum);
  }
  
  /**
   * Raw datum setting. 
   */
  protected void setItem2(byte[] datum) {
    item2.set(datum,0,datum.length);
  }
  
  protected void setItem2(byte[] datum,int offset,int length) {
    item2.set(datum,offset,length);
  }
  
  protected void setItem2(BytesWritable datum) {
    item2.set(datum);
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    item1.write(out);
    item2.write(out);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
  	item1.readFields(in);
  	item2.readFields(in);
  }
  
  /** True iff "binary" equals */
  public boolean equals(Object o) {
    if (!(o instanceof DatumPairBase))
      return false;
    DatumPairBase other = (DatumPairBase) o;
    return (this.item1.equals(other.item1) && this.item2.equals(other.getItem2()));
  }
  
  /** Compares two RawComparable. */
  public int compareTo(Object o) {
  	DatumPairBase other = (DatumPairBase) o;
  	int res = this.item1.compareTo(other.getItem1());
  	
  	if (res == 0) {
  		return this.item2.compareTo(other.getItem2());
  	} else {
  		return res;
  	}
  }
  
  public int hashCode() {
  	return (31*item1.hashCode() + item2.hashCode()) & Integer.MAX_VALUE;
  }

  /** A Comparator optimized for PairDatumRawComparable. 
   * 
   * */ 
  public static class ComparatorWithNoOrder extends WritableComparator {
    public ComparatorWithNoOrder() {
      super(DatumPairBase.class);
    }

    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
    	return WritableComparator.compareBytes(b1, s1, l1,
          b2, s2, l2);
    }
  }

  static {                                        // register this comparator
    WritableComparator.define(DatumPairBase.class, new ComparatorWithNoOrder());
  }
}


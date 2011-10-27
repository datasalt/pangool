package com.datasalt.pangolin.mapred.crossproduct.io;


import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.thrift.TException;

import com.datasalt.pangolin.io.DatumPairBase;
import com.datasalt.pangolin.mapred.crossproduct.CrossProductMapRed;

/**
 * Used as output of {@link CrossProductMapRed}
 * @author epalace
 *
 */
public class CrossProductPair extends DatumPairBase{
//Default constructor needed.
  public CrossProductPair() {
    super();
  }
  
  public CrossProductPair(byte [] left, byte[] right) throws TException {
    super(left,right);
  }
    
  /**
   * Raw datum getter
   */
  public BytesWritable getLeft() {
  	return getItem1();
  }
  
  /**
   * Raw datum getter
   */
  public BytesWritable getRight() {
  	return getItem2();
  }
    
  /**
   * Raw datum setting. 
   */
  public void setLeft(byte[] item) {
    setItem1(item);
  }
  
  public void setLeft(byte[] datum,int offset,int length) {
    setItem1(datum,offset,length);
  }
  
  public void setLeft(BytesWritable datum) {
    setItem1(datum);
  }
  
  
  /**
   * Raw datum setting. 
   */
  public void setRight(byte[] datum) {
    setItem2(datum);
  }
  
  public void setRight(byte[] datum,int offset,int length) {
    setItem2(datum,offset,length);
  }
  
  public void setRight(BytesWritable datum) {
    setItem2(datum);
  }
	
  static {                                        // register this comparator
    WritableComparator.define(CrossProductPair.class, new ComparatorWithNoOrder());
  }
  
}

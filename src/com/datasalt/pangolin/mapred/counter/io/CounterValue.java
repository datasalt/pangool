package com.datasalt.pangolin.mapred.counter.io;


import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.thrift.TException;

import com.datasalt.pangolin.io.DatumPairBase;

public final class CounterValue extends DatumPairBase{

//Default constructor needed.
  public CounterValue() {
    super();
  }
  
  public CounterValue(byte [] item, byte[] count) throws TException {
    super(item,count);
  }
    
  /**
   * Raw datum getter
   */
  public BytesWritable getItem() {
  	return getItem1();
  }
  
  /**
   * Raw datum getter
   */
  public BytesWritable getCount() {
  	return getItem2();
  }
    
  /**
   * Raw datum setting. 
   */
  public void setItem(byte[] item) {
    setItem1(item);
  }
  
  public void setItem(byte[] datum,int offset,int length) {
    setItem1(datum,offset,length);
  }
  
  public void setItem(BytesWritable datum) {
    setItem1(datum);
  }
  
  
  /**
   * Raw datum setting. 
   */
  public void setCount(byte[] datum) {
    setItem2(datum);
  }
  
  public void setCount(byte[] datum,int offset,int length) {
    setItem2(datum,offset,length);
  }
  
  public void setCount(BytesWritable datum) {
    setItem2(datum);
  }
	
  static {                                        // register this comparator
    WritableComparator.define(CounterValue.class, new ComparatorWithNoOrder());
  }
	
}

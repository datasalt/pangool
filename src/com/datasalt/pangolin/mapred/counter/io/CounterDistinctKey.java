package com.datasalt.pangolin.mapred.counter.io;


import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparator;

import com.datasalt.pangolin.io.IdDatumBase;

public class CounterDistinctKey extends IdDatumBase
{
	
  public CounterDistinctKey() {
    super();
  }
  
  public CounterDistinctKey(int groupId, byte[] group) {
  	super(groupId,group);
  }
    
  public int getGroupId() {
  	return getIdentifier();
  }
  
  public void setGroupId(int groupId) {
  	setIdentifier(groupId);
  }
  
  /**
   * Raw datum getter
   */
  public BytesWritable getGroup() {
  	return getItem1();
  }
  
  
  public void setGroup(byte[] datum,int offset,int length){
    setItem1(datum,offset,length);
  }
  
  public void setGroup(BytesWritable writable){
    setItem1(writable);
  }
  
  /**
   * Raw datum setting. 
   */
  public void setGroup(byte[] datum){
    setItem1(datum);
  }
  
  static {                                        // register this comparator
    WritableComparator.define(CounterDistinctKey.class, new Comparator());
  }

}

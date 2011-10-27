package com.datasalt.pangolin.mapred.crossproduct.io;


import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.thrift.TException;

import com.datasalt.pangolin.io.IdDatumBase;

public class CrossProductExtraKey extends IdDatumBase
{
	
  public CrossProductExtraKey() {
    super();
  }
  
  public CrossProductExtraKey(int groupId, byte[] group) throws TException {
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
    WritableComparator.define(CrossProductExtraKey.class, new Comparator());
  }

}

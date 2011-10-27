package com.datasalt.pangolin.mapred.counter.io;



import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparator;

import com.datasalt.pangolin.io.IdDatumPairBase;

/**
 * A class used in the {@link MapRedCounter}
 * 
 * @author epalace
 */
public class CounterKey extends IdDatumPairBase{

	public CounterKey(){
		super();
	}
	
	 public CounterKey(int groupId, byte[] group, byte[] item) {
	  	super(groupId,group,item);
	  }
	  
	 
	  
	  public void set(int groupId, byte[] group, byte[] item) {
	  	super.set(groupId, group, item);
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
	  
	  /**
	   * Raw datum getter
	   */
	  public BytesWritable getItem() {
	  	return getItem2();
	  }
	  
	  /**
	   * Raw datum setting. 
	   */
	  
	  public void setGroup(BytesWritable b){
	    setItem1(b);
	  }
	  
	  public void setItem(BytesWritable b){
	    setItem2(b);
	  }
	  
	  public void setGroup(byte[] datum)  {
	    setItem1(datum);
	  }
	  
	  public void setGroup(byte[] datum,int offset,int length)  {
	    setItem1(datum,offset,length);
	  }

	  public void setItem(byte[] datum)  {
	    setItem2(datum);
	  }
	  
	  
	  /**
	   * Raw datum setting. 
	   */
	  public void setItem(byte[] datum,int offset,int length) {
	    setItem2(datum,offset,length);
	  }
	  
	  
	  /** A Comparator optimized for PairDatumRawComparable that only compares
	   * by the group Id and the group. */ 
	  public static class IdGroupComparator extends IdDatumPairBase.IdItem1Comparator {}
	  
	  /**
	   * Partitioner class that decides the partition only using the field
	   * typeIdentifier and item1. Needed to do properly the secondary sorting.
	   *  
	   * @author eric
	   */
	  public static class IdGroupPartitioner extends IdDatumPairBase.IdItem1Partitioner{}
	  
	  
	  
	  static {                                        // register this comparator
	    WritableComparator.define(CounterKey.class, new Comparator());
	  } 
	
	
}

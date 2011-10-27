package com.datasalt.pangolin.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * A class with a number, that can be used as an identifier of the types
 * of the data that the other item, stored as byte array, belongs to. These arrays can be
 * objects serialized with the {@link Serialization}.
 * <br/>
 * The advantages of this class is that the comparison is done at the binary level,
 * without deserializing the item. That can be useful for using this class as key 
 * on a Map Reduce job.
 * <br/>
 * Item must be present and cannot be null.
 * <p> 
 * This class have the getters and setters as protected. To use
 * this class, override and create your own setters (recommended
 * for the understanding of the code), or use the class {@link IdDatum} 

 * @author ivan,eric
 *
 */
@SuppressWarnings("rawtypes")
public class IdDatumBase implements WritableComparable {

	private int identifier=0;
  private BytesWritable item1=new BytesWritable();
  
  // Default constructor needed.
  public IdDatumBase() {
    
  }
  
  public IdDatumBase(int identifier, byte[] item1) {
  	setIdentifier(identifier);
    setItem1(item1);
  }
    
  protected int getIdentifier() {
  	return identifier;
  }
  
  protected void setIdentifier(int identifier) {
  	this.identifier = identifier;
  }
  
  /**
   * Raw datum getter
   */
  protected BytesWritable getItem1() {
  	return item1;
  }
  
  protected void setItem1(byte[] datum,int offset,int length){
    item1.set(datum,offset,length);
  }
  
  protected void setItem1(BytesWritable writable){
    item1.set(writable);
  }
  
  /**
   * Raw datum setting. 
   */
  protected void setItem1(byte[] datum){
    item1.set(datum,0,datum.length);
  }
    
  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, identifier);
  	item1.write(out);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
  	identifier = WritableUtils.readVInt(in);
  	item1.readFields(in);
  }
  
  

  /** True iff "binary" equals */
  public boolean equals(Object o) {
    if (!(o instanceof IdDatumBase)){
      return false;
    }
    IdDatumBase other = (IdDatumBase) o;
    return (identifier == other.identifier && this.item1.equals(other.item1)); 
  }
  
  /** Compares two RawComparable. */
  public int compareTo(Object o) {
  	IdDatumBase other = (IdDatumBase) o;
  	if (identifier == other.identifier){
  		return item1.compareTo(other.item1); 
  	} else {
  		return (identifier > other.identifier ) ? 1 : -1 ;
  	}
  	
  }
 
  public int hashCode() {
  	return (identifier * 31 + item1.hashCode())& Integer.MAX_VALUE;
  }
 
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(IdDatumBase.class);
    }

    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
    	return WritableComparator.compareBytes(b1, s1, l1,
          b2, s2, l2);
    }
  }

  static {                                        // register this comparator
    WritableComparator.define(IdDatumBase.class, new Comparator());
  }
}


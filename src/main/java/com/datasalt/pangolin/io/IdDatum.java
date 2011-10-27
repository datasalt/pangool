package com.datasalt.pangolin.io;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparator;

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
 *
 * @author ivan,eric
 *
 */
public class IdDatum extends IdDatumBase {

  static {                                        // register this comparator
    WritableComparator.define(IdDatum.class, new Comparator());
  }

	@Override
  public int getIdentifier() {
	  return super.getIdentifier();
  }

	@Override
  public void setIdentifier(int identifier) {
	  super.setIdentifier(identifier);
  }

	@Override
  public BytesWritable getItem1() {
	  return super.getItem1();
  }

	@Override
  public void setItem1(byte[] datum, int offset, int length) {
	  super.setItem1(datum, offset, length);
  }

	@Override
  public void setItem1(BytesWritable writable) {
	  super.setItem1(writable);
  }

	@Override
  public void setItem1(byte[] datum) {
	  super.setItem1(datum);
  }
}


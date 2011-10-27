package com.datasalt.pangolin.io;


import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparator;

import com.datasalt.pangolin.io.IdDatumBase.Comparator;

/**
 * A class with two items, stored as byte arrays. These arrays can be
 * objects serialized with the {@link Serialization}.
 * <br/>
 * The advantages of this pair is that the comparison is done at the binary level,
 * without deserializing the items. That can be useful for using this class as key 
 * on a Map Reduce job.
 * <br/>
 * Both items must be present and cannot be null. 
 *
 * @author ivan
 *
 */
public class DatumPair extends DatumPairBase {
	
  static {                                        // register this comparator
    WritableComparator.define(DatumPair.class, new Comparator());
  }

	@Override
  public BytesWritable getItem1() {
	  return super.getItem1();
  }

	@Override
  public BytesWritable getItem2() {
	  return super.getItem2();
  }

	@Override
  public void setItem1(byte[] datum) {
	  super.setItem1(datum);
  }

	@Override
  public void setItem1(byte[] datum, int offset, int length) {
	  super.setItem1(datum, offset, length);
  }

	@Override
  public void setItem1(BytesWritable datum) {
	  super.setItem1(datum);
  }

	@Override
  public void setItem2(byte[] datum) {
	  super.setItem2(datum);
  }

	@Override
  public void setItem2(byte[] datum, int offset, int length) {
	  super.setItem2(datum, offset, length);
  }

	@Override
  public void setItem2(BytesWritable datum) {
	  super.setItem2(datum);
  }
	
}


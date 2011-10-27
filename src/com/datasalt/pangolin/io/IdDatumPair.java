package com.datasalt.pangolin.io;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparator;

/**
 * A class with a number, that can be used as an identifier of the types of the
 * data that the other two items, stored as byte arrays, belongs to. These
 * arrays can be objects serialized with the {@link Serialization}. <br/>
 * The advantages of this pair is that the comparison is done at the binary
 * level, without deserializing the items. That can be useful for using this
 * class as key on a Map Reduce job. <br/>
 * Both items must be present and cannot be null.
 * 
 * @author ivan,eric
 * 
 */
public class IdDatumPair extends IdDatumPairBase {

	static { // register this comparator
		WritableComparator.define(IdDatumPair.class, new Comparator());
	}

	@Override
  protected int getIdentifier() {
	  return super.getIdentifier();
  }

	@Override
  protected BytesWritable getItem1() {
	  return super.getItem1();
  }

	@Override
  protected BytesWritable getItem2() {
	  return super.getItem2();
  }

	@Override
  protected void setItem1(BytesWritable b) {
	  super.setItem1(b);
  }

	@Override
  protected void setItem2(BytesWritable b) {
	  super.setItem2(b);
  }

	@Override
  protected void setItem1(byte[] datum) {
	  super.setItem1(datum);
  }

	@Override
  protected void setItem1(byte[] datum, int offset, int length) {
	  super.setItem1(datum, offset, length);
  }

	@Override
  protected void setItem2(byte[] datum) {
	  super.setItem2(datum);
  }

	@Override
  protected void setItem2(byte[] datum, int offset, int length) {
	  super.setItem2(datum, offset, length);
  }

}

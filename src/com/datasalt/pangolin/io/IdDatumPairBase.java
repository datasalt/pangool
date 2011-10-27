package com.datasalt.pangolin.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * A class with a number, that can be used as an identifier of the types of the
 * data that the other two items, stored as byte arrays, belongs to. These
 * arrays can be objects serialized with the {@link Serialization}. <br/>
 * The advantages of this pair is that the comparison is done at the binary
 * level, without deserializing the items. That can be useful for using this
 * class as key on a Map Reduce job. <br/>
 * Both items must be present and cannot be null.
 * <p>
 * This class have the getters and setters as protected. To use
 * this class, override and create your own setters (recommended
 * for the understanding of the code), or use the class {@link IdDatumPair}.
 * 
 * @author ivan,eric
 * 
 */
@SuppressWarnings("rawtypes")
public class IdDatumPairBase implements WritableComparable {
	private static final int PRIME = 16777619;
	private static final int SIZE_INT = 4;
	private int identifier;
	private DatumPairBase datumPair = new DatumPairBase();

	// Default constructor needed.
	public IdDatumPairBase() {

	}

	public IdDatumPairBase(int identifier, byte[] item1, byte[] item2) {
		setIdentifier(identifier);
		setItem1(item1);
		setItem2(item2);
	}

	public static IdDatumPairBase newOne(int identifier, byte[] item1, byte[] item2) {
		IdDatumPairBase pair = new IdDatumPairBase();
		pair.set(identifier, item1, item2);
		return pair;
	}

	public void set(int identifier, byte[] item1, byte[] item2) {
		setIdentifier(identifier);
		setItem1(item1);
		setItem2(item2);
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
		return datumPair.getItem1();
	}

	/**
	 * Raw datum getter
	 */
	protected BytesWritable getItem2() {
		return datumPair.getItem2();
	}

	/**
	 * Raw datum setting.
	 */

	protected void setItem1(BytesWritable b) {
		datumPair.setItem1(b);
	}

	protected void setItem2(BytesWritable b) {
		datumPair.setItem2(b);
	}

	protected void setItem1(byte[] datum) {
		datumPair.setItem1(datum);
	}

	protected void setItem1(byte[] datum, int offset, int length) {
		datumPair.setItem1(datum, offset, length);
	}

	protected void setItem2(byte[] datum) {
		datumPair.setItem2(datum);
	}

	/**
	 * Raw datum setting.
	 */
	protected void setItem2(byte[] datum, int offset, int length) {
		datumPair.setItem2(datum, offset, length);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVInt(out, identifier);
		datumPair.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		identifier = WritableUtils.readVInt(in);
		datumPair.readFields(in);
	}

	/** True iff "binary" equals */
	public boolean equals(Object o) {
		if(!(o instanceof IdDatumPairBase))
			return false;
		IdDatumPairBase other = (IdDatumPairBase) o;
		return(identifier == other.getIdentifier() && this.datumPair.equals(other.datumPair));
	}

	/** Compares two RawComparable. */
	public int compareTo(Object o) {
		IdDatumPairBase other = (IdDatumPairBase) o;
		if(this.identifier == other.identifier) {
			return this.datumPair.compareTo(other.datumPair);
		} else {
			return identifier > other.identifier ? 1 : -1;
		}
	}

	public int hashCode() {
		return (identifier * PRIME + datumPair.hashCode()) & Integer.MAX_VALUE;
	}

	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(IdDatumPairBase.class, true);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				int idb1 = readVInt(b1, s1);
				int idb2 = readVInt(b2, s2);

				if(idb1 != idb2) {
					return(idb1 > idb2 ? 1 : -1);
				}
				int offset = WritableUtils.decodeVIntSize(b1[s1]);

				int sizeArray1 = readInt(b1, s1 + offset);
				int sizeArray2 = readInt(b2, s2 + offset);
				int offset1 = offset + SIZE_INT;
				int offset2 = offset + SIZE_INT;
				int cmp = compareBytes(b1, s1 + offset1, sizeArray1, b2, s2 + offset2, sizeArray2);
				if(cmp != 0) {
					return cmp;
				}
				offset1 = offset1 + sizeArray1;
				offset2 = offset2 + sizeArray2;
				sizeArray1 = readInt(b1, s1 + offset1);
				sizeArray2 = readInt(b2, s2 + offset1);
				offset1 = offset1 + SIZE_INT;
				offset2 = offset2 + SIZE_INT;
				return compareBytes(b1, s1 + offset1, sizeArray1, b2, s2 + offset2, sizeArray2);

			} catch(IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	static { // register this comparator
		WritableComparator.define(IdDatumPairBase.class, new Comparator());
	}

	/**
	 * A Comparator optimized for PairDatumRawComparable that only compares by the
	 * identifier and the item1.
	 */
	public static class IdItem1Comparator extends WritableComparator {
		public IdItem1Comparator() {
			super(IdDatumPairBase.class, true);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {

				int idb1 = readVInt(b1, s1);
				int idb2 = readVInt(b2, s2);

				if(idb1 != idb2) {
					return(idb1 > idb2 ? 1 : -1);
				}
				int offset = WritableUtils.decodeVIntSize(b1[s1]);

				int sizeArray1 = readInt(b1, s1 + offset);
				int sizeArray2 = readInt(b2, s2 + offset);
				int offset1 = offset + SIZE_INT;
				int offset2 = offset + SIZE_INT;
				return compareBytes(b1, s1 + offset1, sizeArray1, b2, s2 + offset2, sizeArray2);

			} catch(IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * Partitioner class that decides the partition only using the field
	 * typeIdentifier and item1. Needed to do properly the secondary sorting.
	 * 
	 * @author ivan,eric
	 */
	public static class IdItem1Partitioner extends Partitioner<IdDatumPairBase, DatumPairBase> {

		@Override
		public int getPartition(IdDatumPairBase groupItem, DatumPairBase item, int partition) {
			int hash = groupItem.identifier * PRIME + groupItem.getItem1().hashCode();
			// Important to use abs: the result cannot be negative
			return (hash & Integer.MAX_VALUE) % partition;
		}

	}

}

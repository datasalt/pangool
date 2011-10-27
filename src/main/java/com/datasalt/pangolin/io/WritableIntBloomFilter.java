package com.datasalt.pangolin.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;

import com.datasalt.pangolin.commons.BloomFilter;

/**
 * A Writable version ready to be used by Hadoop of {@link BloomFilter}
 * It has fixed 32-bit length so it can be serialized to an int.
 * 
 * @author pere
 *
 */
public class WritableIntBloomFilter<E> implements Writable {

	BloomFilter<E> filter;
	short byteSize;
	int nElements;
	
	public WritableIntBloomFilter() {

	}
	
	@Override
  public void readFields(DataInput in) throws IOException {
		this.byteSize = in.readShort();
		this.nElements = in.readInt();
		setBloomFilter(byteSize, nElements);
		byte[] toRead = new byte[byteSize];
		for(int i = 0; i < byteSize; i++) {
			toRead[i] = in.readByte();
		}
		fromByteArray(toRead);
  }

	@Override
  public void write(DataOutput out) throws IOException {
		out.writeShort(byteSize);
		out.writeInt(filter.getExpectedNumberOfElements());
		byte[] byteArray = toByteArray();
		for(byte b: byteArray) {
			out.write(b);
		}
  }

	public void setBloomFilter(short byteSize, int nElements) {
		BloomFilter<E> filter = new BloomFilter<E>(byteSize * 8, nElements);
		this.nElements = nElements;
		this.byteSize = byteSize;
		this.filter = filter;
	}
	
	public void fromByteArray(byte[] bytes) { 
		for(int j = 0; j < bytes.length; j++) {
			for(int i = 0; i < 8; i++) {
				if((bytes[j] & (1 << i)) != 0) {
					filter.getBitSet().set(j * 8 + i);
				}
			}
		}
	}
	
	public byte[] toByteArray() {
		byte[] bytes = new byte[byteSize];
		for(int j = 0; j < byteSize; j++) {
			byte toSerialize = 0;
			for(int i = 0; i < 8; i++) {
		  	if(filter.getBitSet().get(j * 8 + i)) {
		  		toSerialize |= (1 << i);
		  	}
		  }		
			bytes[j] = toSerialize;
		}
		return bytes;
	}
	
	public boolean contains(E element) {
		if(byteSize == 0) {
			return true;
		}
		return filter.contains(element);
	}
	
	public void add(E element) {
		if(byteSize == 0) {
			return;
		}
		filter.add(element);
	}
	
	public double getFalsePositiveProbability() {
		if(byteSize == 0) {
			return 1.0;
		}
		return filter.getFalsePositiveProbability();
	}
	
	public boolean equals(Object obj) {
		if(!getClass().equals(obj.getClass())) {
			return false;
		}
		@SuppressWarnings("rawtypes")
    WritableIntBloomFilter secondFilter = (WritableIntBloomFilter)obj;
		return Arrays.equals(toByteArray(), secondFilter.toByteArray());
	}
	
	public int hashCode() {
		return Arrays.hashCode(toByteArray());
	}
	
	public WritableIntBloomFilter<String> deepCopy() {
		WritableIntBloomFilter<String> wI = new WritableIntBloomFilter<String>();
		wI.setBloomFilter(byteSize, nElements);
		wI.fromByteArray(toByteArray());
		return wI;
	}
}

package com.datasalt.pangolin.commons;

/**
 * Provides strictly increasing timestamps. Returns a milliseconds timestamp padded with 6 digits that will increase in case
 * that the same timestamp is requested
 * 
 * @author pere
 *
 */
public class TimestampOracle {

	private long lastTimestamp = getTimestamp();
	int paddingBits = 10;
	private int nRepetitions = 0;
	
	public long getUniqueTimestamp() {
		long timestamp = getTimestamp() << paddingBits;
		if(timestamp == lastTimestamp) {
			nRepetitions++;
			timestamp += nRepetitions;
		} else {
			nRepetitions = 0;
			lastTimestamp = timestamp;
		}
		return timestamp;
	}
	
	
	
	private void setPadding(){
		int maxBitsPadding = getMaxPaddingBits();
		this.paddingBits = Math.min(maxBitsPadding,paddingBits);
		
	}
	
	public static int getMaxPaddingBits(){
		int maxBits =(int) Math.floor(Math.log(Long.MAX_VALUE/System.currentTimeMillis())/Math.log(2));
		return maxBits;
	}
	
	public TimestampOracle(){	
		setPadding();
	}
	
	
	public TimestampOracle(int paddingBits){
		this.paddingBits = paddingBits;
		setPadding();
	}
	
	public static long getTimestamp() { // test implementations might override this
		return System.currentTimeMillis();
	}
	
	public static void main(String[] args){
		TimestampOracle tmp = new TimestampOracle();
		System.out.println("Timestamp : " + TimestampOracle.getTimestamp());
		System.out.println("Unique timestamp : " + tmp.getUniqueTimestamp());
		System.out.println("MAX LONG SIZE : " + Long.MAX_VALUE);
		System.out.println(Math.log(Long.MAX_VALUE/TimestampOracle.getTimestamp())/Math.log(2));
		
		int nShards = 10;
		int bitsForShard = (int)Math.ceil(numBits(nShards));
		System.out.println("Bits for shards " + bitsForShard);
		int bitsForCurrentMillis = (int)Math.ceil(numBits(System.currentTimeMillis()));
		System.out.println("Bits used for current time millis " + bitsForCurrentMillis);
		int bitsInLong = Long.SIZE -1; // -1 because just positive
		System.out.println("Bits in long " + bitsInLong);
		int padding = bitsInLong - bitsForCurrentMillis - bitsForShard;
		System.out.println("Padding : " + padding);
		System.out.println((1l << 62) + (1l << 61));
		
	}
	
	public static double numBits(long num){
		return Math.log(num)/Math.log(2);
	}
	
	/*
	public static TimestampOracle createWithNBitsUnused(int nbits){
		int usefulbits = Long.SIZE - 1 -nbits;
		
	}*/
}
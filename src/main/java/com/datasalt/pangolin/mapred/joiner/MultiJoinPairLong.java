package com.datasalt.pangolin.mapred.joiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparator;

/**
 * {@link MultiJoinPair} for having custom secondary sorting in 
 * the {@link MultiJoiner}. Use method {@link MultiJoiner#setMultiJoinPairClass(Class)}
 * for configuring a {@link MultiJoiner} job with this class.
 * <p>
 * With this class we can secondary-sort ascendantly by a long. 
 * 
 * @author ivan
 */
public class MultiJoinPairLong extends MultiJoinPair<LongWritable>{

	public MultiJoinPairLong() throws InstantiationException, IllegalAccessException {
	  super(LongWritable.class);
  }
	
	public static class Comparator extends MultiJoinPair.Comparator {

		public Comparator() {
	    super(LongWritable.class);
    }
	}
	
	static {
		WritableComparator.define(MultiJoinPairLong.class, new Comparator());
	}
}

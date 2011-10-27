package com.datasalt.pangolin.mapred.joiner;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparator;

/**
 * {@link MultiJoinPair} for having custom secondary sorting in 
 * the {@link MultiJoiner}. Use method {@link MultiJoiner#setMultiJoinPairClass(Class)}
 * for configuring a {@link MultiJoiner} job with this class.
 * <p>
 * With this class we can secondary-sort ascendantly by a double. 
 * 
 * @author pere
 */
public class MultiJoinPairDouble extends MultiJoinPair<DoubleWritable>{

	public MultiJoinPairDouble() throws InstantiationException, IllegalAccessException {
	  super(DoubleWritable.class);
  }
	
	public static class Comparator extends MultiJoinPair.Comparator {

		public Comparator() {
	    super(DoubleWritable.class);
    }
	}
	
	static {
		WritableComparator.define(MultiJoinPairDouble.class, new Comparator());
	}
}


package com.datasalt.pangolin.mapred.joiner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

/**
 * {@link MultiJoinPair} for having custom secondary sorting in 
 * the {@link MultiJoiner}. Use method {@link MultiJoiner#setMultiJoinPairClass(Class)}
 * for configuring a {@link MultiJoiner} job with this class.
 * <p>
 * With this class we can secondary-sort lexicographically. 
 * 
 * @author pere
 */
public class MultiJoinPairText extends MultiJoinPair<Text>{

	public MultiJoinPairText() throws InstantiationException, IllegalAccessException {
	  super(Text.class);
  }
	
	public static class Comparator extends MultiJoinPair.Comparator {

		public Comparator() {
	    super(Text.class);
    }
	}
	
	static {
		WritableComparator.define(MultiJoinPairText.class, new Comparator());
	}
}

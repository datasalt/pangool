package com.datasalt.pangolin.mapred.joiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparator;

public class MultiJoinPairInt extends MultiJoinPair<IntWritable>{

	public MultiJoinPairInt() throws InstantiationException, IllegalAccessException {
	  super(IntWritable.class);
  }
	
	public static class Comparator extends MultiJoinPair.Comparator {

		public Comparator() {
	    super(IntWritable.class);
    }
	}
	
	static {
		WritableComparator.define(MultiJoinPairInt.class, new Comparator());
	}
}

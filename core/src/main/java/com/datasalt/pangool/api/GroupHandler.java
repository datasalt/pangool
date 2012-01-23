package com.datasalt.pangool.api;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.datasalt.pangolin.grouper.Grouper;
import com.datasalt.pangool.CoGrouper;
import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.PangoolConfig;
import com.datasalt.pangool.io.tuple.ITuple;
import com.datasalt.pangool.mapreduce.RollupReducer;
import com.datasalt.pangool.mapreduce.SimpleReducer;

/**
 * 
 * This is the common interface that any {@link CoGrouper} job needs to implement. This handler is called in the reducer
 * step by {@link SimpleReducer} or {@link RollupReducer} depending if Roll-up feature is used.
 * 
 * @author eric
 * 
 */
@SuppressWarnings("rawtypes")
public class GroupHandler<OUTPUT_KEY, OUTPUT_VALUE> {

	// To be added state info here, 
	public static class State {
		private PangoolConfig pangoolConfig;
		
		public State(PangoolConfig pangoolConfig) {
			this.pangoolConfig = pangoolConfig;
		}		
		
		public PangoolConfig getPangoolConfig() {
			return pangoolConfig;
		}
	}
	
	public void setup(State state, ReduceContext<ITuple, NullWritable, OUTPUT_KEY, OUTPUT_VALUE> context) throws IOException, InterruptedException, CoGrouperException {

	}

	public void cleanup(State state, ReduceContext<ITuple, NullWritable, OUTPUT_KEY, OUTPUT_VALUE> context) throws IOException, InterruptedException,
	    CoGrouperException {
	}

	/**
	 * 
	 * This is method is called with an iterable that contains all the tuples that have been grouped by the fields defined
	 * in {@link Grouper#setFieldsToGroupBy(String...)}
	 * 
	 * @param tuples
	 *          Iterable that contains all the tuples from a group
	 * @param context
	 *          The reducer context as in {@link Reducer}
	 */
	public void onGroupElements(ITuple group, Iterable<ITuple> tuples, State state, ReduceContext<ITuple, NullWritable, OUTPUT_KEY, OUTPUT_VALUE> context) throws IOException,
	    InterruptedException, CoGrouperException {
		
	}
}

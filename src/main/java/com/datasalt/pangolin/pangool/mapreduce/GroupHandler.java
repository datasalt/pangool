package com.datasalt.pangolin.pangool.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.datasalt.pangolin.grouper.io.tuple.ITuple;
import com.datasalt.pangolin.grouper.mapreduce.RollupReducer;
import com.datasalt.pangolin.grouper.mapreduce.SimpleReducer;
import com.datasalt.pangolin.pangool.CoGrouperException;
import com.datasalt.pangolin.pangool.Schema;

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

	/**
	 * 
	 * @param schema
	 *          The schema from the tuples
	 * @param context
	 *          See {@link Reducer.Context}
	 */
	public void setup(Schema schema, Reducer.Context context) throws IOException, InterruptedException, CoGrouperException {

	}

	public void cleanup(Schema schema, Reducer.Context context) throws IOException, InterruptedException,
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
	public void onGroupElements(Iterable<ITuple> tuples, Context context) throws IOException,
	    InterruptedException, CoGrouperException {
		
	}

}

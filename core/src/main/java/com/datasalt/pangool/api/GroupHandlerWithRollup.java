package com.datasalt.pangool.api;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import com.datasalt.pangool.CoGrouperException;
import com.datasalt.pangool.Grouper;
import com.datasalt.pangool.io.tuple.ITuple;

@SuppressWarnings("serial")
public class GroupHandlerWithRollup<OUTPUT_KEY, OUTPUT_VALUE> extends GroupHandler<OUTPUT_KEY, OUTPUT_VALUE> {

	/**
	 * 
	 * This is the method called any time that a sub-group is opened when rollup is used. Check {@link Grouper} doc about
	 * how roll-up feature works
	 * 
	 * @param depth
	 *          The tuple's field index that is currently being opened.0 when it's the first field
	 * @param field
	 *          The tuple's field name that is currently being opened.
	 * @param firstElement
	 *          The first tuple from the current group
	 * @param context
	 *          The reducer context as in {@link Reducer}
	 * 
	 */
	public void onOpenGroup(int depth, String field, ITuple firstElement, CoGrouperContext context, Collector collector)
	    throws IOException, InterruptedException, CoGrouperException {
	}

	/**
	 * 
	 * This is the method called after every sub-group is being closed when rollup is used. Check {@link Grouper} doc
	 * about how roll-up feature works
	 * 
	 * @param depth
	 *          The tuple's field index that is currently being opened.It's 0 when it's the first field
	 * @param field
	 *          The tuple's field name that is currently being opened.
	 * @param firstElement
	 *          The last tuple from the current group
	 * @param context
	 *          The reducer context as in {@link Reducer}
	 * 
	 */
	public void onCloseGroup(int depth, String field, ITuple lastElement,
	    CoGrouperContext context, Collector collector) throws IOException, InterruptedException, CoGrouperException {

	}
}

package com.datasalt.pangool.flow.ops;

import java.io.IOException;

import com.datasalt.pangool.io.ITuple;

/**
 * Operation that counts the number of tuples that are in a group of tuples and sets the result as a long Object in the specified tuple field.
 */
@SuppressWarnings("serial")
public class Count extends TupleReduceOp {

	String destField;
	
	public Count(String destField) {
		this.destField = destField;
	}

	public void process(Iterable<ITuple> tuples, ReturnCallback<ITuple> callback) throws IOException,
	    InterruptedException {

	  long count = 0l;
	  ITuple lastTuple = null;
		for(ITuple tuple : tuples) {
			count ++;
			lastTuple = tuple;
		}
		lastTuple.set(destField, count);
		callback.onReturn(lastTuple);
	}
}
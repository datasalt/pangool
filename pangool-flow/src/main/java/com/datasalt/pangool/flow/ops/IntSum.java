package com.datasalt.pangool.flow.ops;

import java.io.IOException;

import com.datasalt.pangool.io.ITuple;

/**
 * Operation that sums a certain integer field from a group of Tuples and sets the result of the sum into a certain field.
 */
@SuppressWarnings("serial")
public class IntSum extends TupleReduceOp {

	private String origField;
	private String destField;
	
	public IntSum(String field) {
		this(field, field);
	}
	
	/**
	 * The destination field is the same than the field we use to sum.
	 */
	public IntSum(String origField, String destField) {
		this.origField = origField;
		this.destField = destField;
	}

	public void process(Iterable<ITuple> tuples, ReturnCallback<ITuple> callback) throws IOException,
	    InterruptedException {

	  int count = 0;
	  ITuple lastTuple = null;
		for(ITuple tuple : tuples) {
			count += (Integer) tuple.get(origField);
			lastTuple = tuple;
		}
		lastTuple.set(destField, count);
		callback.onReturn(lastTuple);
	}
}

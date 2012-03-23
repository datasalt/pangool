package com.datasalt.pangool.flow.ops;

import java.io.IOException;

import com.datasalt.pangool.flow.Utils;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;

@SuppressWarnings("serial")
public class Count extends TupleOp<Iterable<ITuple>> {

	String destField;
	Schema copySchema;
	
	public Count(String destField, Schema outSchema, Schema copySchema) {
		super(outSchema);
		this.destField = destField;
		this.copySchema = copySchema;
	}

	public void process(Iterable<ITuple> tuples, ReturnCallback<ITuple> callback) throws IOException,
	    InterruptedException {

	  long count = 0l;
	  ITuple lastTuple = null;
		for(ITuple tuple : tuples) {
			count ++;
			lastTuple = tuple;
		}
		Utils.shallowCopy(lastTuple, tuple, copySchema);
		tuple.set(destField, count);
		callback.onReturn(tuple);
	}
}
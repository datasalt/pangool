package com.datasalt.pangool.flow.ops;

import java.io.IOException;

import com.datasalt.pangool.flow.Utils;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;

@SuppressWarnings("serial")
public class LongSum extends TupleReduceOp {

	String origField;
	String destField;
	Schema copySchema;

	public LongSum(String field, Schema outSchema) {
		this(field, field, outSchema, outSchema);
	}
	
	public LongSum(String origField, String destField, Schema outSchema) {
		this(origField, destField, outSchema, outSchema);
	}
	
	public LongSum(String field, Schema outSchema, Schema copySchema) {
		this(field, field, outSchema, copySchema);
	}
	
	public LongSum(String origField, String destField, Schema outSchema, Schema copySchema) {
		super(outSchema);
		this.origField = origField;
		this.destField = destField;
		this.copySchema = copySchema;
	}

	public void process(Iterable<ITuple> tuples, ReturnCallback<ITuple> callback) throws IOException,
	    InterruptedException {

	  long count = 0l;
	  ITuple lastTuple = null;
		for(ITuple tuple : tuples) {
			count += (Long) tuple.get(origField);
			lastTuple = tuple;
		}
		Utils.shallowCopy(lastTuple, tuple, copySchema);
		tuple.set(destField, count);
		callback.onReturn(tuple);
	}
}

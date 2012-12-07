package com.datasalt.pangool.flow.ops;

import java.io.IOException;

import com.datasalt.pangool.flow.Utils;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;

@SuppressWarnings("serial")
public class IntSum extends TupleReduceOp {

	String origField;
	String destField;
	Schema copySchema;
	
	public IntSum(String field, Schema schema) {
		this(field, field, schema, schema);
	}
	
	public IntSum(String field, Schema outSchema, Schema copySchema) {
		this(field, field, outSchema, copySchema);
	}

	public IntSum(String origField, String destField, Schema outSchema, Schema copySchema) {
		super(outSchema);
		this.origField = origField;
		this.destField = destField;
		this.copySchema = copySchema;
	}

	public void process(Iterable<ITuple> tuples, ReturnCallback<ITuple> callback) throws IOException,
	    InterruptedException {

	  int count = 0;
	  ITuple lastTuple = null;
		for(ITuple tuple : tuples) {
			count += (Integer) tuple.get(origField);
			lastTuple = tuple;
		}
		Utils.shallowCopy(lastTuple, tuple, copySchema);
		tuple.set(destField, count);
		callback.onReturn(tuple);
	}
}

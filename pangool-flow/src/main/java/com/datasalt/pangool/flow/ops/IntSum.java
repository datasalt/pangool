package com.datasalt.pangool.flow.ops;

import java.io.IOException;

import com.datasalt.pangool.flow.Utils;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;

@SuppressWarnings("serial")
public class IntSum extends TupleReduceOp {

	String origField;
	String destField;
	String[] copyFields;
	
	public IntSum(String field, Schema schema) {
		this(field, field, schema, Utils.getFieldNames(schema));
	}
	
	public IntSum(String field, Schema outSchema, String... copyFields) {
		this(field, field, outSchema, copyFields);
	}

	public IntSum(String origField, String destField, Schema outSchema, String... copyFields) {
		super(outSchema);
		this.origField = origField;
		this.destField = destField;
		this.copyFields = copyFields;
	}

	public void process(Iterable<ITuple> tuples, ReturnCallback<ITuple> callback) throws IOException,
	    InterruptedException {

	  int count = 0;
	  ITuple lastTuple = null;
		for(ITuple tuple : tuples) {
			count += (Integer) tuple.get(origField);
			lastTuple = tuple;
		}
		Utils.shallowCopy(lastTuple, tuple, copyFields);
		tuple.set(destField, count);
		callback.onReturn(tuple);
	}
}

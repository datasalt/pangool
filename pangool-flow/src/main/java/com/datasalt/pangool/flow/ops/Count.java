package com.datasalt.pangool.flow.ops;

import java.io.IOException;

import com.datasalt.pangool.flow.Utils;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;

@SuppressWarnings("serial")
public class Count extends TupleReduceOp {

	String destField;
	String[] copyFields;
	
	public Count(String destField, Schema outSchema) {
		this(destField, outSchema, Utils.getFieldNames(outSchema));
	}
	
	public Count(String destField, Schema outSchema, String... copyFields) {
		super(outSchema);
		this.destField = destField;
		this.copyFields = copyFields;
	}

	public void process(Iterable<ITuple> tuples, ReturnCallback<ITuple> callback) throws IOException,
	    InterruptedException {

	  long count = 0l;
	  ITuple lastTuple = null;
		for(ITuple tuple : tuples) {
			count ++;
			lastTuple = tuple;
		}
		Utils.shallowCopy(lastTuple, tuple, copyFields);
		tuple.set(destField, count);
		callback.onReturn(tuple);
	}
}
package com.datasalt.pangool.flow.ops;

import java.io.IOException;

import com.datasalt.pangool.flow.Utils;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;

@SuppressWarnings("serial")
public class LongSum extends TupleReduceOp {

	String origField;
	String destField;
	String[] copyFields;

	public LongSum(String field, Schema outSchema) {
		this(field, field, outSchema, Utils.getFieldNames(outSchema));
	}
	
	public LongSum(String origField, String destField, Schema outSchema) {
		this(origField, destField, outSchema, Utils.getFieldNames(outSchema));
	}
	
	public LongSum(String field, Schema outSchema, String... copyFields) {
		this(field, field, outSchema, copyFields);
	}
	
	public LongSum(String origField, String destField, Schema outSchema, String... copyFields) {
		super(outSchema);
		this.origField = origField;
		this.destField = destField;
		this.copyFields = copyFields;
	}

	public void process(Iterable<ITuple> tuples, ReturnCallback<ITuple> callback) throws IOException,
	    InterruptedException {

	  long count = 0l;
	  ITuple lastTuple = null;
		for(ITuple tuple : tuples) {
			count += (Long) tuple.get(origField);
			lastTuple = tuple;
		}
		Utils.shallowCopy(lastTuple, tuple, copyFields);
		tuple.set(destField, count);
		callback.onReturn(tuple);
	}
}
